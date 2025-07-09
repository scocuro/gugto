#!/usr/bin/env python3
# real_estate_report.py

import argparse
import os
import sys
import requests
import pandas as pd
from datetime import datetime
from io import StringIO

# ── 1) 시 군 구 코드 CSV 로드 ──
CSV_PATH = "code_raw.csv"
try:
    csv_df = pd.read_csv(CSV_PATH, encoding="euc-kr", dtype=str)
except Exception as e:
    print(f"ERROR: 시군구 코드 CSV를 불러오는 데 실패했습니다 ({CSV_PATH}): {e}")
    sys.exit(1)

# ── 2) region_name → 5자리 코드 매핑 ──
def get_region_code(region_name: str) -> str:
    parts = region_name.split()
    sido = parts[0]
    if len(parts) == 2:
        sigungu = parts[1]
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == sigungu) &
            (csv_df["읍면동명"].isna())
        ]
    elif len(parts) == 3:
        full_sigungu = parts[1] + parts[2]
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == full_sigungu)
        ]
    else:
        raise ValueError("‘시도 시군구’ 또는 ‘시도 시군구 읍면동’ 형식으로 입력해 주세요.")
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")
    return sub.iloc[0]["법정동코드"][:5]

# ── 3) CLI 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
grp = parser.add_mutually_exclusive_group(required=True)
grp.add_argument('--lawd-cd',     help='5자리 시군구코드를 직접 입력')
grp.add_argument('--region-name', help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
parser.add_argument('--start',     required=True,              help='조회 시작 시점 (YYYYMM)')
parser.add_argument('--end',       default=None,               help='조회 종료 시점 (YYYYMM), 미지정 시 현재월')
parser.add_argument('--min-area',  type=float, default=None,   help='조회 최소 면적 (㎡)')
parser.add_argument('--max-area',  type=float, default=None,   help='조회 최대 면적 (㎡)')
parser.add_argument('--output',    default='report.xlsx',      help='출력 엑셀 파일명')
args = parser.parse_args()

# ── 4) 시군구코드 결정 ──
if args.lawd_cd:
    region_code = args.lawd_cd
else:
    try:
        region_code = get_region_code(args.region_name)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)

# ── 5) API 키 & 엔드포인트 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)

BASE_SALE_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
BASE_RENT_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
BASE_SILV_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade"

# ── 6) 기간 파싱 ──
try:
    start_year, start_month = int(args.start[:4]), int(args.start[4:6])
    if args.end:
        end_year, end_month = int(args.end[:4]), int(args.end[4:6])
    else:
        today = datetime.today()
        end_year, end_month = today.year, today.month
except Exception:
    print("ERROR: --start, --end 값은 YYYYMM 형식이어야 합니다.")
    sys.exit(1)

# ── 7) fetch items 헬퍼 ──
def fetch_items(url, params):
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        df = pd.read_xml(StringIO(r.text), xpath=".//item", parser="etree")
        return df.to_dict(orient="records")
    except Exception:
        return []

# ── 8) 전체 수집 함수 ──
def collect_all(base_url, cols, date_key):
    all_rows = []
    for yy in range(start_year, end_year + 1):
        m_start = start_month if yy == start_year else 1
        m_end   = end_month   if yy == end_year   else 12
        for mm in range(m_start, m_end + 1):
            ymd = f"{yy}{mm:02d}"
            page = 1
            while True:
                params = {
                    'serviceKey': API_KEY,
                    'LAWD_CD':    region_code,
                    date_key:     ymd,
                    'pageNo':     page,
                    'numOfRows':  1000,
                    'resultType': 'xml'
                }
                recs = fetch_items(base_url, params)
                if not recs:
                    break
                df = pd.DataFrame(recs)
                df = df.loc[:, [*cols, 'dealYear', 'dealMonth', 'dealDay']]

                # 면적 숫자 변환 및 필터링
                if 'excluUseAr' in df.columns:
                    df['excluUseAr'] = (
                        df['excluUseAr']
                          .astype(str)
                          .str.replace(',','',regex=False)
                          .astype(float)
                    )
                    # 사용자 지정 면적 범위 적용
                    if args.min_area is not None:
                        df = df[df['excluUseAr'] >= args.min_area]
                    if args.max_area is not None:
                        df = df[df['excluUseAr'] <= args.max_area]
                    # 평단위 보정값
                    df['excluUseAr_adj'] = df['excluUseAr'] * 121/400

                # 금액/보증금/월세 숫자 변환
                if 'dealAmount' in df.columns:
                    df['dealAmount'] = (
                        df['dealAmount']
                          .astype(str)
                          .str.replace(',','',regex=False)
                          .astype(float)
                    )
                if 'deposit' in df.columns:
                    df['deposit'] = (
                        df['deposit']
                          .astype(str)
                          .str.replace(',','',regex=False)
                          .astype(float)
                    )
                if 'monthlyRent' in df.columns:
                    df['monthlyRent'] = (
                        df['monthlyRent']
                          .astype(str)
                          .str.replace(',','',regex=False)
                          .astype(float)
                    )

                all_rows.append(df)
                page += 1

    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()

# ── 9) 컬럼 리스트 정의 ──
sale_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','dealAmount','buildYear']
rent_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','deposit','monthlyRent','contractType']
silv_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','dealAmount']

# ── 10) 데이터 수집 ──
print("▶ 매매 수집…")
df_sale = collect_all(BASE_SALE_URL, sale_cols, 'DEAL_YMD')
print(f"  → {len(df_sale)}건 수집 완료")

print("▶ 전월세 수집…")
df_rent = collect_all(BASE_RENT_URL, rent_cols, 'DEAL_YMD')
print(f"  → {len(df_rent)}건 수집 완료")

print("▶ 분양권 수집…")
df_silv = collect_all(BASE_SILV_URL, silv_cols, 'DEAL_YMD')
print(f"  → {len(df_silv)}건 수집 완료")

# ── 11) 연도별 컬럼 확장 피벗 ──
def make_pivot(df, valcol):
    if df.empty:
        return pd.DataFrame()
    df2 = df.copy()
    if 'excluUseAr_adj' not in df2.columns:
        df2['excluUseAr_adj'] = pd.NA
    agg = df2.groupby(['umdNm','aptNm','dealYear']).agg(
        거래건수=('dealYear','size'),
        평균거래가액=(valcol,'mean'),
        평균전용면적=('excluUseAr_adj','mean')
    ).reset_index()
    pv = agg.pivot(index=['umdNm','aptNm'], columns='dealYear')
    pv.columns = [f"{str(year)[-2:]}_{metric}" for metric, year in pv.columns]
    return pv.reset_index()

# ── 12) 엑셀 작성 ──
with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    df_sale.to_excel(writer, sheet_name='매매(raw)',   index=False)
    df_rent.to_excel(writer, sheet_name='전세(raw)',   index=False)
    df_silv.to_excel(writer, sheet_name='분양권(raw)', index=False)

    make_pivot(df_sale, 'dealAmount').to_excel(writer, sheet_name='매매(수정)', index=False)
    make_pivot(df_rent, 'deposit').to_excel(writer, sheet_name='전세(수정)', index=False)
    make_pivot(df_silv, 'dealAmount').to_excel(writer, sheet_name='분양권(수정)', index=False)

print(f"✅ '{args.output}' 에 저장되었습니다.")
