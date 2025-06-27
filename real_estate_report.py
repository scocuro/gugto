"""
real_estate_report.py

공공데이터 실거래 Raw Data 수집기
  • 시군구 코드 CSV 로드
  • --lawd-cd / --region-name, --start-year, --built-after, --output
  • TLS1.2 고정 게이트웨이 호출
  • XML 네임스페이스 제거 후 파싱
  • 페이징으로 전체 Raw 레코드 수집
  • 건축년도 필터
  • 필요한 컬럼만 남기기
  • 수집 건수 월별·총계 출력
  • Excel에 Raw Data 저장
"""

import os
import sys
import ssl
import re
import argparse
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

# ─────────────────────────────────────────────────────────────────────────────
# 1) 시군구 코드 CSV 로드
CSV_PATH = "code_raw.csv"
try:
    csv_df = pd.read_csv(CSV_PATH, dtype=str, encoding="utf-8")
except Exception as e:
    print(f"ERROR: 시군구 코드 CSV 로드 실패 ({CSV_PATH}): {e}")
    sys.exit(1)

def get_region_code(region_name: str) -> str:
    parts = region_name.strip().split()
    if len(parts) == 2:
        sido, sigungu = parts
        sub = csv_df[
            (csv_df["시도명"]   == sido) &
            (csv_df["시군구명"] == sigungu) &
            (csv_df["읍면동명"].isna())
        ]
    elif len(parts) == 3:
        sido, city, gu = parts
        full = city + gu
        sub = csv_df[
            (csv_df["시도명"]   == sido) &
            (csv_df["시군구명"] == full)
        ]
    else:
        raise ValueError("‘시도 종군구’ 또는 ‘시도 시군구 읍면동’ 형식으로 입력해 주세요.")
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 찾을 수 없습니다.")
    return sub.iloc[0]["법정동코드"][:5]

# ─────────────────────────────────────────────────────────────────────────────
# 2) 커맨드라인 인자
parser = argparse.ArgumentParser(description="공공데이터 실거래 Raw Data 수집기")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--lawd-cd',     help='5자리 법정동(시군구) 코드 직접 입력')
group.add_argument('--region-name', help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
parser.add_argument('--start-year',  type=int, default=2025, help='조회 시작 연도')
parser.add_argument('--built-after', type=int, default=2015, help='준공 연도 이후 필터')
parser.add_argument('--output',      default='raw_report.xlsx', help='출력 엑셀 파일명')
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
built_after = args.built_after
output_file = args.output

# ─────────────────────────────────────────────────────────────────────────────
# 3) API Key 및 TLS1.2 세션 설정
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)

class TLS12Adapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

session = requests.Session()
session.mount("https://", TLS12Adapter())

BASE_URL = (
    "https://apis.data.go.kr/1613000/"
    "RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
)
PAGE_SIZE = 1000

# ─────────────────────────────────────────────────────────────────────────────
# 4) 수집 → 필요한 컬럼만 필터
WANT_COLS = [
    'sggCd', 'umdNm', 'aptNm', 'jibun',
    'excluUseAr','dealYear','dealMonth','dealDay',
    'dealAmount','floor','buildYear'
]

def fetch_transactions(lawd_cd: str, deal_ym: str, page_no: int) -> pd.DataFrame:
    params = {
        'serviceKey': API_KEY,
        'LAWD_CD':    lawd_cd,
        'DEAL_YMD':   deal_ym,
        'pageNo':     page_no,
        'numOfRows':  PAGE_SIZE,
    }
    resp = session.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()

    # XML 네임스페이스 제거
    text = resp.content.decode('utf-8')
    text = re.sub(r'\sxmlns="[^"]+"', '', text, count=1)

    df = pd.read_xml(text, xpath='.//item', parser='lxml')

    # 컬럼명 한글 매핑
    df.rename(columns={
        'sggCd':       'sggCd',
        'umdNm':       'umdNm',
        'aptNm':       'aptNm',
        'jibun':       'jibun',
        'excluUseAr':  'excluUseAr',
        'dealYear':    'dealYear',
        'dealMonth':   'dealMonth',
        'dealDay':     'dealDay',
        'dealAmount':  'dealAmount',
        'floor':       'floor',
        'buildYear':   'buildYear',
    }, inplace=True)

    # 타입 변환
    if 'dealAmount' in df:
        df['dealAmount'] = df['dealAmount'].str.replace(',', '').astype(float)
    df['excluUseAr'] = df['excluUseAr'].astype(float)
    df['buildYear']  = df['buildYear'].astype(int)

    # 건축년도 필터
    df = df[df['buildYear'] >= built_after]

    # 필요한 컬럼만
    return df[WANT_COLS]

# ─────────────────────────────────────────────────────────────────────────────
# 5) 전체 Raw 데이터 수집 & 카운트 집계
records = []
counts = {}  # 월별 건수

current_year  = datetime.now().year
current_month = datetime.now().month

for year in range(start_year, current_year + 1):
    last_month = current_month if year == current_year else 12
    for month in range(1, last_month + 1):
        ym = f"{year}{month:02d}"
        month_total = 0
        page = 1
        while True:
            try:
                df = fetch_transactions(region_code, ym, page)
            except Exception as e:
                print(f"WARN: {ym} p{page} 호출/파싱 실패: {e}")
                break
            if df.empty:
                break
            records.append(df)
            cnt = len(df)
            month_total += cnt
            if cnt < PAGE_SIZE:
                break
            page += 1
        counts[ym] = month_total
        print(f"[INFO] {ym} 수집건수 = {month_total}")

all_data = pd.concat(records, ignore_index=True) if records else pd.DataFrame()

print("\n=== 월별 건수 요약 ===")
for ym, cnt in sorted(counts.items()):
    print(f"  {ym}: {cnt}")
print(f"총 거래 건수: {len(all_data)}건\n")

if all_data.empty:
    print("❌ 조건에 맞는 Raw 데이터가 없습니다.")
    sys.exit(0)

# ─────────────────────────────────────────────────────────────────────────────
# 6) Raw Data 엑셀 저장
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    all_data.to_excel(writer, sheet_name='RawData', index=False)

print(f"✅ Raw Data가 '{output_file}' 로 저장되었습니다.")
