#!/usr/bin/env python3
# real_estate_report.py

import argparse
import os
import sys
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO

# ── 설정값 ──
CSV_PATH = "code_raw.csv"       # 시군구 코드 CSV 파일 경로 (프로젝트 루트)
rent_conversion_rate = 0.06     # 6%

# ── 1) 시군구 코드 CSV 로드 ──
try:
    csv_df = pd.read_csv(CSV_PATH, encoding="utf-8", dtype=str)
except Exception as e:
    print(f"ERROR: 시군구 코드 CSV 로드 실패 ({CSV_PATH}): {e}")
    sys.exit(1)

def get_region_code(region_name: str) -> str:
    parts = region_name.split()
    sido = parts[0]
    if len(parts) == 2:
        sigungu = parts[1]
        sub = csv_df[
            (csv_df["시도명"]   == sido) &
            (csv_df["시군구명"] == sigungu) &
            (csv_df["읍면동명"].isna())
        ]
    elif len(parts) == 3:
        city, gu = parts[1], parts[2]
        sub = csv_df[
            (csv_df["시도명"]   == sido) &
            (csv_df["시군구명"] == city + gu)
        ]
    else:
        raise ValueError("‘시도 종군구’ 또는 ‘시도 시군구 읍면동’ 형식으로 입력해 주세요.")
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")
    return sub.iloc[0]["법정동코드"][:5]

# ── 2) 커맨드라인 인자 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--lawd-cd',     help='법정동코드(5자리)')
group.add_argument('--region-name', help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
parser.add_argument('--start-year', type=int, default=2020, help='조회 시작 연도')
parser.add_argument('--output',     default='report.xlsx', help='출력 엑셀 파일명')
args = parser.parse_args()

if args.lawd_cd:
    region_code = args.lawd_cd
else:
    try:
        region_code = get_region_code(args.region_name)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)

start_year  = args.start_year
output_file = args.output

# ── 3) API 키 및 엔드포인트 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: 환경변수 PUBLIC_DATA_API_KEY에 API 키를 설정하세요.")
    sys.exit(1)

BASE_SALE_URL = (
    "http://apis.data.go.kr/1613000/"
    "RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
)
BASE_RENT_URL = (
    "http://apis.data.go.kr/1613000/"
    "RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
)
BASE_SILV_URL = (
    "http://apis.data.go.kr/1613000/"
    "RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade"
)

def collect_all(base_url: str, cols: list, date_key: str) -> pd.DataFrame:
    records = []
    this_year = datetime.now().year
    for yy in range(start_year, this_year + 1):
        for mm in range(1, 13):
            ymd = f"{yy}{mm:02d}"
            page = 1
            while True:
                params = {
                    "serviceKey": API_KEY,
                    "LAWD_CD":    region_code,
                    date_key:     ymd,
                    "pageNo":     page,
                    "numOfRows":  1000,
                    "resultType": "xml"
                }
                try:
                    r = requests.get(base_url, params=params, timeout=30)
                    r.raise_for_status()
                    df = pd.read_xml(
                        BytesIO(r.content),
                        xpath=".//item",
                        parser="lxml"
                    )
                except Exception as e:
                    print(f"[WARNING] {ymd} p{page} 요청 실패: {e}")
                    break
                if df.empty:
                    break

                # ── 컬럼 슬라이싱 ──
                df = df.reindex(columns=cols).copy()

                # ── 숫자형 컬럼 클린업 ──
                for numcol in ["dealAmount","deposit","monthlyRent","excluUseAr","floor","buildYear"]:
                    if numcol in df.columns:
                        df[numcol] = (
                            df[numcol]
                              .astype(str)
                              .str.replace(",", "", regex=False)
                              .replace("", "0")
                              .astype(float)
                        )

                # ── 전월세 전용 계산 ──
                if "monthlyRent" in df.columns:
                    df["fixed_deposit"] = (
                        df["monthlyRent"] * 12 / rent_conversion_rate
                        + df["deposit"]
                    )

                # ── excluUseAr 보정 ──
                if "excluUseAr" in df.columns:
                    df["excluUseAr_adj"] = df["excluUseAr"] * 121 / 400

                records.append(df)
                if len(df) < 1000:
                    break
                page += 1

    if records:
        return pd.concat(records, ignore_index=True)
    else:
        extra = []
        if "monthlyRent" in cols:
            extra.append("fixed_deposit")
        if "excluUseAr" in cols:
            extra.append("excluUseAr_adj")
        return pd.DataFrame(columns=cols + extra)

# ── 4) 컬럼 정의 ──
sale_cols = [
    "sggCd","umdNm","aptNm","jibun",
    "excluUseAr","dealYear","dealMonth","dealDay",
    "dealAmount","floor","buildYear"
]
rent_cols = [
    "sggCd","umdNm","aptNm","jibun",
    "excluUseAr","dealYear","dealMonth","dealDay",
    "floor","buildYear","deposit","monthlyRent"
]
silv_cols = sale_cols + ["ownershipGbn"]

print("▶ 매매(Sales) 수집 중…")
df_sale = collect_all(BASE_SALE_URL, sale_cols, "DEAL_YMD")
print(f"  → {len(df_sale)}건 수집 완료")

print("▶ 전월세(Rent) 수집 중…")
df_rent = collect_all(BASE_RENT_URL, rent_cols, "DEAL_YMD")
print(f"  → {len(df_rent)}건 수집 완료")

print("▶ 분양권(Silver) 수집 중…")
df_silv = collect_all(BASE_SILV_URL, silv_cols, "DEAL_YMD")
print(f"  → {len(df_silv)}건 수집 완료")

# ── 5) 엑셀 저장 ──
with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    # 원본 시트
    df_sale.to_excel(writer, sheet_name="매매(raw)", index=False)
    df_rent.to_excel(writer, sheet_name="전월세(raw)", index=False)
    df_silv.to_excel(writer, sheet_name="분양권(raw)", index=False)

    # ── 피벗 테이블 생성 함수 ──
    def make_pivot(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        pivot = (
            df.groupby(["umdNm","aptNm","dealYear"])
              .agg(
                  case_count=("dealYear","size"),
                  avg_value =(value_col,"mean"),
                  avg_exclu =("excluUseAr_adj","mean")
              )
              .unstack("dealYear")
        )
        pivot.columns = ["_".join(map(str,c)) for c in pivot.columns]
        return pivot.reset_index()

    # 수정본 시트
    make_pivot(df_sale, "dealAmount") \
        .to_excel(writer, sheet_name="매매(수정)", index=False)
    make_pivot(df_rent, "fixed_deposit") \
        .to_excel(writer, sheet_name="전세(수정)", index=False)
    make_pivot(df_silv, "dealAmount") \
        .to_excel(writer, sheet_name="분양권(수정)", index=False)

print(f"✅ '{output_file}' 생성 완료")
