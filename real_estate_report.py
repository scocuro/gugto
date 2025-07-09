#!/usr/bin/env python3
# real_estate_report.py (with debug logging)

import argparse
import os
import sys
import requests
import pandas as pd
from datetime import datetime
from io import StringIO

# ── 1) 시군구 코드 CSV 로드 ──
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
    code5 = sub.iloc[0]["법정동코드"][0:5]
    print(f">>> DEBUG: region_name='{region_name}' → region_code='{code5}'")
    return code5

# ── 3) CLI 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기 (debug)")
grp = parser.add_mutually_exclusive_group(required=True)
grp.add_argument('--lawd-cd',     help='5자리 시군구코드를 직접 입력')
grp.add_argument('--region-name', help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
parser.add_argument('--start-year', type=int, default=2020, help='조회 시작 연도')
parser.add_argument('--output',     default='report.xlsx', help='출력 엑셀 파일명')
args = parser.parse_args()

# ── 4) 시군구코드 결정 ──
if args.lawd_cd:
    region_code = args.lawd_cd
    print(f">>> DEBUG: lawd_cd provided → region_code='{region_code}'")
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

# ── 6) fetch items 헬퍼 (with debug) ──
def fetch_items(url, params):
    # log request
    print(f">>> DEBUG: fetch_items url={url}")
    print(f"    params={params}")
    try:
        r = requests.get(url, params=params, timeout=30)
        print(f">>> DEBUG: full_request_url={r.url}")
        r.raise_for_status()
    except Exception as e:
        print(f">>> DEBUG: fetch_items error: {e}")
        return []
    txt = r.text
    print(f">>> DEBUG: status={r.status_code}, content-type={r.headers.get('Content-Type')}")
    print(f"    text_preview={txt[:200]!r}")
    try:
        df = pd.read_xml(StringIO(txt), xpath='.//item', parser='etree')
    except Exception as e:
        print(f">>> DEBUG: XML parse error: {e}")
        return []
    print(f">>> DEBUG: parsed_records={len(df)}")
    return df.to_dict(orient='records')

# ── 7) 전체 수집 함수 (with debug) ──
def collect_all(base_url, cols, date_key):
    today = datetime.today()
    all_rows = []
    for yy in range(args.start_year, today.year + 1):
        max_m = today.month if yy == today.year else 12
        for mm in range(1, max_m + 1):
            ymd = f"{yy}{mm:02d}"
            page = 1
            print(f">>> DEBUG: collect_all ymd={ymd}")
            while True:
                print(f">>> DEBUG: collect_all page={page}")
                params = {
                    'serviceKey': API_KEY,
                    'LAWD_CD':    region_code,
                    date_key:     ymd,
                    'pageNo':     page,
                    'numOfRows':  1000,
                    'resultType': 'xml'
                }
                recs = fetch_items(base_url, params)
                print(f">>> DEBUG: received {len(recs)} records")
                if not recs:
                    print(f">>> DEBUG: no more data for ymd={ymd}, page={page}")
                    break
                df = pd.DataFrame(recs)
                print(f">>> DEBUG: raw df columns={df.columns.tolist()}")
                df = df.loc[:, [*cols, 'dealYear', 'dealMonth', 'dealDay']]
                # numeric conversions
                if 'dealAmount' in df:
                    df['dealAmount'] = (df['dealAmount']
                        .astype(str).str.replace(',','',regex=False)
                        .astype(float))
                if 'deposit' in df:
                    df['deposit'] = (df['deposit']
                        .astype(str).str.replace(',','',regex=False)
                        .astype(float))
                if 'monthlyRent' in df:
                    df['monthlyRent'] = (df['monthlyRent']
                        .astype(str).str.replace(',','',regex=False)
                        .astype(float))
                if 'excluUseAr' in df:
                    df['excluUseAr_adj'] = (
                        df['excluUseAr'].astype(str)
                                       .str.replace(',','',regex=False)
                                       .astype(float)
                        * 121/400
                    )
                print(f">>> DEBUG: cleaned df shape={df.shape}")
                all_rows.append(df)
                page += 1
    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()

# ── 8) 컬럼 리스트 정의 ──
sale_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','dealAmount','buildYear']
rent_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','deposit','monthlyRent','contractType']
silv_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','dealAmount']

# ── 9) 데이터 수집 ──
print("▶ 매매 수집…")
df_sale = collect_all(BASE_SALE_URL, sale_cols, 'DEAL_YMD')
print(f"  → {len(df_sale)}건 수집 완료")
print("▶ 전월세 수집…")
df_rent = collect_all(BASE_RENT_URL, rent_cols, 'DEAL_YMD')
print(f"  → {len(df_rent)}건 수집 완료")
print("▶ 분양권 수집…")
df_silv = collect_all(BASE_SILV_URL, silv_cols, 'DEAL_YMD')
print(f"  → {len(df_silv)}건 수집 완료")

# ── 10) 엑셀 작성 ──
with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    df_sale.to_excel(writer, sheet_name='매매(raw)',   index=False)
    df_rent.to_excel(writer, sheet_name='전세(raw)',   index=False)
    df_silv.to_excel(writer, sheet_name='분양권(raw)', index=False)

    # Pivot 함수: 항상 기본 컬럼 존재만 가정
    def make_pivot(df, valcol):
        if df.empty:
            return pd.DataFrame()
        g = df.groupby(['umdNm','aptNm','dealYear'], dropna=False)
        agg_kwargs = {
            '거래건수': ('dealYear','size'),
            '평균거래가액': (valcol,'mean')
        }
        if 'excluUseAr_adj' in df.columns:
            agg_kwargs['평균전용면적(평)'] = ('excluUseAr_adj','mean')
        pv = g.agg(**agg_kwargs).reset_index()
        return pv

    make_pivot(df_sale, 'dealAmount').to_excel(writer, sheet_name='매매(수정)', index=False)
    make_pivot(df_rent, 'deposit').to_excel(writer, sheet_name='전세(수정)', index=False)
    make_pivot(df_silv, 'dealAmount').to_excel(writer, sheet_name='분양권(수정)', index=False)

print(f"✅ '{args.output}' 에 저장되었습니다.")
