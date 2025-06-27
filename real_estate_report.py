import argparse
import os
import sys
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO

# ── 설정값 ──
CSV_PATH = "code_raw.csv"         # 시군구 코드 CSV
rent_conversion_rate = 0.06       # 0.06 = 6%

# ── 1) 시군구 코드 CSV 로드 ──
try:
    csv_df = pd.read_csv(CSV_PATH, encoding="utf-8", dtype=str)
except Exception as e:
    print(f"ERROR: 시군구 코드 CSV를 불러오는 데 실패했습니다 ({CSV_PATH}): {e}")
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

# ── 2) CLI 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
grp = parser.add_mutually_exclusive_group(required=True)
grp.add_argument('--lawd-cd',     help='법정동코드(앞5자리)')
grp.add_argument('--region-name', help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
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

# ── 3) API 키 & 엔드포인트 ──
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

def collect_all(base_url: str, cols: list, key_ym: str) -> pd.DataFrame:
    """주어진 API를 start_year~현재년까지 한 달씩 수집"""
    rows = []
    current_year = datetime.now().year
    for year in range(start_year, current_year+1):
        for month in range(1,13):
            ymd = f"{year}{month:02d}"
            page = 1
            while True:
                params = {
                    "serviceKey": API_KEY,
                    "LAWD_CD":    region_code,
                    key_ym:       ymd,
                    "pageNo":     page,
                    "numOfRows":  1000,
                    "resultType": "xml"
                }
                try:
                    r = requests.get(base_url, params=params, timeout=30)
                    r.raise_for_status()
                    # XML → DataFrame
                    df = pd.read_xml(BytesIO(r.content),
                                     xpath=".//item",
                                     parser="lxml")
                except Exception as e:
                    print(f"[WARNING] {ymd} p{page} failed: {e}")
                    break
                if df.empty:
                    break
                # 필요한 칼럼만 필터
                df = df.loc[:, cols]
                # 전월세만 부가 계산
                if "monthlyRent" in df.columns:
                    df["fixed_deposit"] = (
                        df["monthlyRent"].astype(float)*12
                        / rent_conversion_rate
                        + df["deposit"].astype(float)
                    )
                # excluUseAr 조정 칼럼
                if "excluUseAr" in df.columns:
                    df["excluUseAr_adj"] = df["excluUseAr"].astype(float)*121/400
                rows.append(df)
                if len(df) < 1000:
                    break
                page += 1
    if rows:
        return pd.concat(rows, ignore_index=True)
    else:
        return pd.DataFrame(columns=cols + (["fixed_deposit","excluUseAr_adj"]
                                           if "monthlyRent" in cols else []))

# ── 4) 원본 데이터 수집 ──
sale_cols = ["sggCd","umdNm","aptNm","jibun",
             "excluUseAr","dealYear","dealMonth",
             "dealDay","dealAmount","floor","buildYear"]
rent_cols = sale_cols + ["deposit","monthlyRent"]
silv_cols = sale_cols + ["ownershipGbn"]

print("▶ 매매(Sales) 수집 중…")
df_sale = collect_all(BASE_SALE_URL, sale_cols, "DEAL_YMD")
print("▶ 전월세(Rent) 수집 중…")
df_rent = collect_all(BASE_RENT_URL, rent_cols, "DEAL_YMD")
print("▶ 분양권(Silver) 수집 중…")
df_silv = collect_all(BASE_SILV_URL, silv_cols, "DEAL_YMD")

# ── 5) 엑셀 저장 ──
with pd.ExcelWriter(output_file, engine="xlsxwriter") as w:
    # 5-1) Raw 시트
    df_sale.to_excel(w, sheet_name="매매(raw)", index=False)
    df_rent.to_excel(w, sheet_name="전월세(raw)", index=False)
    df_silv.to_excel(w, sheet_name="분양권(raw)", index=False)

    # 5-2) 수정본 피벗 함수
    def make_pivot(df, value_col):
        pv = df.groupby(
            ["umdNm","aptNm","dealYear"]
        ).agg(
            case_count = ("dealYear",    "size"),
            avg_value  = (value_col,      "mean"),
            avg_exclu  = ("excluUseAr_adj","mean")
        ).unstack("dealYear")
        # 컬럼 레벨 정리
        pv.columns = ["_".join(map(str,c)).strip()
                      for c in pv.columns]
        return pv.reset_index()

    # 매매(수정): value_col="dealAmount"
    make_pivot(df_sale, "dealAmount") \
        .to_excel(w, sheet_name="매매(수정)", index=False)
    # 전세(수정): value_col="fixed_deposit"
    make_pivot(df_rent, "fixed_deposit") \
        .to_excel(w, sheet_name="전세(수정)", index=False)
    # 분양권(수정): value_col="dealAmount"
    make_pivot(df_silv, "dealAmount") \
        .to_excel(w, sheet_name="분양권(수정)", index=False)

print(f"✅ 리포트가 '{output_file}' 로 저장되었습니다.")
