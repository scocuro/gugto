#!/usr/bin/env python3
# real_estate_report.py

import argparse
import os
import sys
import requests
import pandas as pd
from datetime import datetime

# ── 1) 시군구 코드 CSV 로드 ──
CSV_PATH = "code_raw.csv"
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
            (csv_df["시도명"] == sido) &
            (csv_df["시군구명"] == sigungu) &
            (csv_df["읍면동명"].isna())
        ]
    elif len(parts) == 3:
        full_sigungu = parts[1] + parts[2]
        sub = csv_df[
            (csv_df["시도명"] == sido) &
            (csv_df["시군구명"] == full_sigungu)
        ]
    else:
        raise ValueError("‘시도 시군구’ 또는 ‘시도 시군구 읍면동’ 형식으로 입력해 주세요.")
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")
    return sub.iloc[0]["법정동코드"][:5]

# ── 2) 커맨드라인 인자 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--lawd-cd',     help='직접 법정동코드(5자리)')
group.add_argument('--region-name', help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
parser.add_argument('--start-year',  type=int, default=2020, help='조회 시작 연도(YYYY)')
parser.add_argument('--output',      default='report.xlsx', help='출력 엑셀 파일명')
args = parser.parse_args()

# ── 3) region_code 결정 ──
if args.lawd_cd:
    region_code = args.lawd_cd
else:
    try:
        region_code = get_region_code(args.region_name)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)

# ── 4) API 키 확인 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: 환경변수 PUBLIC_DATA_API_KEY에 API 키를 설정하세요.")
    sys.exit(1)

PAGE_SIZE = 1000
BASE_SALES_URL = (
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

def fetch_df(base_url: str, ym: str, page: int) -> pd.DataFrame:
    params = {
        'serviceKey': API_KEY,
        'LAWD_CD':    region_code,
        'DEAL_YMD':   ym,
        'pageNo':     page,
        'numOfRows':  PAGE_SIZE,
    }
    resp = requests.get(base_url, params=params, timeout=30)
    resp.raise_for_status()
    return pd.read_xml(resp.content, xpath='.//item', parser='lxml')

def collect(base_url: str, want_cols: list[str]) -> pd.DataFrame:
    all_recs = []
    this_year = datetime.now().year
    for year in range(args.start_year, this_year+1):
        for month in range(1,13):
            ym = f"{year}{month:02d}"
            page = 1
            while True:
                try:
                    df = fetch_df(base_url, ym, page)
                except Exception as e:
                    print(f"WARN: {ym} p{page} 요청 실패: {e}")
                    break
                if df.empty:
                    break
                # 숫자형 컬럼 변환
                for col in ['dealAmount','excluUseAr','deposit','monthlyRent']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(
                            df[col].astype(str).str.replace(',',''),
                            errors='coerce'
                        )
                sub = df[[c for c in want_cols if c in df.columns]].copy()
                all_recs.append(sub)
                if len(df) < PAGE_SIZE:
                    break
                page += 1
    if all_recs:
        return pd.concat(all_recs, ignore_index=True)
    else:
        return pd.DataFrame(columns=want_cols)

# ── 5) 원하는 컬럼 목록 ──
want_sales = [
    'sggCd','umdNm','aptNm','jibun',
    'excluUseAr','dealYear','dealMonth','dealDay',
    'dealAmount','floor','buildYear'
]
want_rent = [
    'sggCd','umdNm','aptNm','jibun',
    'excluUseAr','dealYear','dealMonth','dealDay',
    'deposit','monthlyRent','floor','buildYear',
    'contractType','useRRRight'
]
want_silv = [
    'sggCd','umdNm','aptNm','jibun',
    'excluUseAr','dealYear','dealMonth','dealDay',
    'dealAmount','ownershipGbn'
]

print(f"[INFO] 매매(Sales) 수집 시작… (code={region_code})")
sales_df = collect(BASE_SALES_URL, want_sales)
print(f"[INFO] Sales 총 {len(sales_df)}건")

print(f"[INFO] 전월세(Rent) 수집 시작…")
rent_df  = collect(BASE_RENT_URL, want_rent)
print(f"[INFO] Rent  총 {len(rent_df)}건")

print(f"[INFO] 분양/입주권(Silv) 수집 시작…")
silv_df  = collect(BASE_SILV_URL, want_silv)
print(f"[INFO] Silv  총 {len(silv_df)}건")

# ── 6) 엑셀 쓰기 ──
with pd.ExcelWriter(args.output, engine='xlsxwriter') as w:
    sales_df.to_excel(w, sheet_name='매매(raw)', index=False)
    rent_df .to_excel(w, sheet_name='전월세(raw)', index=False)
    silv_df .to_excel(w, sheet_name='분양권(raw)', index=False)

print(f"✅ '{args.output}' 생성 완료")
