"""
real_estate_report.py

공공데이터 실거래 Raw Data 수집기
  • 시군구 코드 CSV 로드
  • --lawd-cd / --region-name, --start-year, --built-after, --output
  • TLS1.2 고정 게이트웨이 호출
  • XML 네임스페이스 제거 후 파싱
  • 페이징으로 전체 Raw 레코드 수집
  • 전처리 (컬럼명 매핑, 타입 변환, 건축년도 필터)
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
parser.add_argument('--start-year',  type=int, default=2020, help='조회 시작 연도')
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
        # 최소 TLS 1.2
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

    # DataFrame 반환
    return pd.read_xml(text, xpath='.//item', parser='lxml')

# ─────────────────────────────────────────────────────────────────────────────
# 4) 전체 Raw 데이터 수집
records = []
current_year = datetime.now().year

for year in range(start_year, current_year + 1):
    for month in range(1, 13):
        ym = f"{year}{month:02d}"
        page = 1
        while True:
            try:
                df = fetch_transactions(region_code, ym, page)
            except Exception as e:
                print(f"WARN: {ym} p{page} 호출/파싱 실패: {e}")
                break
            if df.empty:
                break

            # 컬럼명 매핑
            df.rename(columns={
                'aptDong':    '법정동',
                'aptNm':      '단지명',
                'dealYear':   '년',
                'dealMonth':  '월',
                'dealAmount': '거래금액',
                'excluUseAr': '전용면적',
                'buildYear':  '건축년도',
            }, inplace=True)

            # 타입 변환
            df['거래금액'] = df['거래금액'].str.replace(',', '').astype(float)
            df['전용면적'] = df['전용면적'].astype(float)
            df['건축년도'] = df['건축년도'].astype(int)

            # 건축년도 필터
            df = df[df['건축년도'] >= built_after]
            if df.empty:
                break

            records.append(df)

            if len(df) < PAGE_SIZE:
                break
            page += 1

# 기록이 없다면 종료
if not records:
    print("❌ 조건에 맞는 Raw 데이터가 없습니다.")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# 5) Raw Data 병합 및 엑셀 저장
all_data = pd.concat(records, ignore_index=True)

with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    all_data.to_excel(writer, sheet_name='RawData', index=False)

print(f"✅ Raw Data가 '{output_file}' 로 저장되었습니다.")
