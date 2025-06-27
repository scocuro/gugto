#!/usr/bin/env python3
# real_estate_report.py

import argparse
import os
import sys
import requests
import pandas as pd
from datetime import datetime

# ── 1) 시군구 코드 CSV 로드 ──
CSV_PATH = "code_raw.csv"  # 프로젝트 루트에 code_raw.csv 위치

try:
    csv_df = pd.read_csv(CSV_PATH, encoding="utf-8", dtype=str)
except Exception as e:
    print(f"ERROR: 시군구 코드 CSV를 불러오는 데 실패했습니다 ({CSV_PATH}): {e}")
    sys.exit(1)

def get_region_code(region_name: str) -> str:
    """
    "충청남도 천안시 동남구" 형태를 받아
    앞 5자리 시군구 코드만 반환.
    """
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
parser.add_argument('--built-after', type=int, default=2015, help='준공 연도 이후 필터')
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

# ── 4) 환경변수에서 API_KEY 획득 ──
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

def fetch_df(base_url: str, deal_ym: str, page: int) -> pd.DataFrame:
    params = {
        'serviceKey': API_KEY,
        'LAWD_CD':    region_code,
        'DEAL_YMD':   deal_ym,
        'pageNo':     page,
        'numOfRows':  PAGE_SIZE,
    }
    resp = requests.get(base_url, params=params, timeout=30)
    resp.raise_for_status()
    # pandas 2.x + lxml 사용
    return pd.read_xml(resp.content, xpath='.//item', parser='lxml')

def collect_all(base_url: str, want_cols: list[str]) -> pd.DataFrame:
    records = []
    this_year = datetime.now().year
    for year in range(args.start_year, this_year + 1):
        for month in range(1, 13):
            ym = f"{year}{month:02d}"
            page = 1
            while True:
                try:
                    df = fetch_df(base_url, ym, page)
                except Exception as e:
                    print(f"WARN: {ym} p{page} 요청/파싱 실패: {e}")
                    break
                if df.empty:
                    break
                # 숫자형 컬럼 안전 변환
                for col in ['dealAmount','excluUseAr','deposit','monthlyRent']:
                    if col in df:
                        df[col] = pd.to_numeric(
                            df[col].astype(str).str.replace(',',''),
                            errors='coerce'
                        )
                # 건축년도 필터
                if 'buildYear' in df:
                    df['buildYear'] = pd.to_numeric(df['buildYear'], errors='coerce').fillna(0).astype(int)
                    df = df[df['buildYear'] >= args.built_after]
                # 필요한 컬럼만 추출
                sub = df[[c for c in want_cols if c in df.columns]].copy()
                records.append(sub)
                if len(df) < PAGE_SIZE:
                    break
                page += 1
    if records:
        return pd.concat(records, ignore_index=True)
    else:
        return pd.DataFrame(columns=want_cols)

# ── 5) 원하는 컬럼 정의 ──
want_cols_sales = [
    'sggCd','umdNm','aptNm','jibun',
    'excluUseAr','dealYear','dealMonth','dealDay',
    'dealAmount','floor','buildYear'
]
want_cols_rent = [
    'sggCd','umdNm','aptNm','jibun',
    'excluUseAr','dealYear','dealMonth','dealDay',
    'deposit','monthlyRent','floor','buildYear',
    'contractType','useRRRight'
]

print(f"[INFO] Sales 수집 시작 (code={region_code})…")
sales_df = collect_all(BASE_SALES_URL, want_cols_sales)
print(f"[INFO] Sales 총 {len(sales_df)}건 수집됨")

print(f"[INFO] Rent 수집 시작 (code={region_code})…")
rent_df  = collect_all(BASE_RENT_URL, want_cols_rent)
print(f"[INFO] Rent 총 {len(rent_df)}건 수집됨")

# ── 6) 엑셀로 저장 ──
with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    sales_df.to_excel(writer, sheet_name='매매(raw)', index=False)
    rent_df .to_excel(writer, sheet_name='전월세(raw)', index=False)

print(f"✅ 리포트가 '{args.output}' 로 저장되었습니다.")
