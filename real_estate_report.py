#!/usr/bin/env python3
# real_estate_report.py

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
    csv_df = pd.read_csv(CSV_PATH, encoding="utf-8", dtype=str)
except Exception as e:
    print(f"ERROR: 시군구 코드 CSV를 불러오는 데 실패했습니다 ({CSV_PATH}): {e}")
    sys.exit(1)

def get_region_code(region_name: str) -> str:
    """
    region_name:
      - '충청남도'
      - '경상남도 진주시'
      - '경기도 수원시 영통구'
    반환: 5자리 시군구 코드 (법정동코드 앞 5자리)
    """
    parts = region_name.split()
    if len(parts) == 1:
        # 광역시도 단위
        sido = parts[0]
        sub = csv_df[csv_df["시도명"] == sido]

    elif len(parts) == 2:
        # 시도 + 시군구
        sido, sigungu = parts
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == sigungu)
        ]

    elif len(parts) == 3:
        # 시도 + 시군구 + 읍면동
        sido, sigungu, eummyundong = parts
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == sigungu) &
            (csv_df["읍면동명"] == eummyundong)
        ]

    else:
        raise ValueError("‘시도’, ‘시도 시군구’, ‘시도 시군구 읍면동’ 형식으로 입력해 주세요.")

    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")

    code5 = sub.iloc[0]["법정동코드"][:5]
    print(f"▶ 입력지역: {region_name} → 시군구코드: {code5}")
    return code5

# ── 2) CLI 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
grp = parser.add_mutually_exclusive_group(required=True)
grp.add_argument('--lawd-cd',     help='5자리 시군구코드를 직접 입력')
grp.add_argument('--region-name', help='시도+시군구[+읍면동] 명칭')
parser.add_argument('--start-year', type=int, default=2020, help='조회 시작 연도')
parser.add_argument('--output',     default='report.xlsx', help='출력 엑셀 파일명')
args = parser.parse_args()

# ── 3) 시군구코드 결정 ──
if args.lawd_cd:
    region_code = args.lawd_cd
else:
    try:
        region_code = get_region_code(args.region_name)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)

# ── 이하 기존 코드 동일 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)

BASE_SALE_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
BASE_RENT_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
BASE_SILV_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcSilvTrade/getRTMSDataSvcAptSilvTrade"

def fetch_items(url, params):
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
    except Exception:
        return []
    txt = r.text
    try:
        df = pd.read_xml(StringIO(txt), xpath='.//item', parser='etree')
    except Exception:
        return []
    return df.to_dict(orient='records')

def collect_all(base_url, cols, date_key):
    today = datetime.today()
    rows = []
    for yy in range(args.start_year, today.year+1):
        max_m = today.month if yy == today.year else 12
        for mm in range(1, max_m+1):
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
                df = df.loc[:, [*cols, 'dealYear','dealMonth','dealDay']]
                if 'dealAmount' in df:
                    df['dealAmount'] = df['dealAmount'].astype(str).str.replace(',','',regex=False).astype(float)
                if 'deposit' in df:
                    df['deposit']    = df['deposit'].astype(str).str.replace(',','',regex=False).astype(float)
                if 'monthlyRent' in df:
                    df['monthlyRent']= df['monthlyRent'].astype(str).str.replace(',','',regex=False).astype(float)
                if 'excluUseAr' in df:
                    df['excluUseAr_adj'] = (
                        df['excluUseAr'].astype(str)
                                       .str.replace(',','',regex=False)
                                       .astype(float)
                        *121/400
                    )
                rows.append(df)
                page += 1
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

sale_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','dealAmount','buildYear']
rent_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','deposit','monthlyRent','contractType']
silv_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','dealAmount']

print("▶ 매매 수집…");   df_sale = collect_all(BASE_SALE_URL, sale_cols, 'DEAL_YMD')
print(f"  → {len(df_sale)}건 수집 완료")
print("▶ 전월세 수집…"); df_rent = collect_all(BASE_RENT_URL, rent_cols, 'DEAL_YMD')
print(f"  → {len(df_rent)}건 수집 완료")
print("▶ 분양권 수집…"); df_silv = collect_all(BASE_SILV_URL, silv_cols, 'DEAL_YMD')
print(f"  → {len(df_silv)}건 수집 완료")

# … 이하 피벗 만들고 엑셀 쓰는 부분 그대로 …
