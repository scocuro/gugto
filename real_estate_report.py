"""
real_estate_report.py

공공데이터 실거래 리포트 생성기
  1) 시군구 코드 CSV 로드
  2) 커맨드라인 인자: --lawd-cd / --region-name, --start-year, --built-after, --output
  3) TLS1.2 고정 게이트웨이 호출 준비
  4) 1) API 직접 호출 테스트 (샘플 한 달)
  5) 전체 기간 페이징 수집 → 건축년도 필터 → 집계 → 엑셀 저장
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
CSV_PATH = "code_raw.csv"   # 프로젝트 루트에 두세요
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
# 2) 커맨드라인 인자 파싱
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--lawd-cd',     help='5자리 법정동(시군구) 코드 직접 입력')
group.add_argument('--region-name', help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
parser.add_argument('--start-year',  type=int, default=2020, help='조회 시작 연도')
parser.add_argument('--built-after', type=int, default=2015, help='준공 연도 이후 필터')
parser.add_argument('--output',      default='report.xlsx', help='출력 엑셀 파일명')
args = parser.parse_args()

# region_code 결정
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
# 3) API Key & TLS1.2 준비
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)

print(f"[DEBUG] using region_code = {region_code}")
print(f"[DEBUG] API_KEY length = {len(API_KEY)}")

class TLS12Adapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.options |= ssl.OP_NO_TLSv1_3
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
    try:
        resp = session.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"HTTP call failed: {e}")

    # XML 네임스페이스 제거
    text = resp.content.decode('utf-8')
    text = re.sub(r'\sxmlns="[^"]+"', '', text, count=1)

    try:
        df = pd.read_xml(text, xpath='.//item', parser='lxml')
    except Exception as e:
        raise RuntimeError(f"XML parse failed: {e}")
    return df

# ─────────────────────────────────────────────────────────────────────────────
# 4) API 직접 호출 테스트 (최근 월 한 달)
now = datetime.now()
# 이전 달 계산
if now.month > 1:
    test_year, test_month = now.year, now.month - 1
else:
    test_year, test_month = now.year - 1, 12
test_ym = f"{test_year}{test_month:02d}"

print(f"\n[TEST] 샘플 호출: {test_ym} 5건만 조회해 봅니다…")
try:
    # numOfRows를 5로 줄여서 빠르게 테스트
    df_test = fetch_transactions(region_code, test_ym, page_no=1)
    print(f"[TEST] status=200, rows={len(df_test)}")
    if not df_test.empty:
        print(df_test.head(2).to_string(index=False))
except Exception as e:
    print(f"[TEST] 호출 또는 파싱 중 오류: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# 5) 전체 기간 데이터 수집
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
                print(f"WARNING: {ym} p{page} failed: {e}")
                break
            if df.empty:
                break
            # 건축년도 필터
            if '건축년도' in df.columns:
                df = df[df['건축년도'].astype(int) >= built_after]
            if df.empty:
                break
            records.append(df)
            if len(df) < PAGE_SIZE:
                break
            page += 1

if not records:
    print("\n❌ 조건에 맞는 거래 데이터가 없습니다. 스크립트를 검토해 보세요.")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# 6) 병합 · 집계
all_data = pd.concat(records, ignore_index=True)

if '년월' in all_data.columns:
    all_data['거래년월'] = pd.to_datetime(
        all_data['년월'].astype(str), format='%Y%m'
    )
    all_data['year'] = all_data['거래년월'].dt.year

if {'전용면적','거래금액'}.issubset(all_data.columns):
    all_data['unit_price'] = (
        all_data['거래금액'].astype(float)
        / all_data['전용면적'].astype(float)
    )

agg = (
    all_data
    .groupby(['법정동','단지명','year'], dropna=False)
    .agg(
        avg_price      = ('거래금액','mean'),
        count          = ('거래금액','size'),
        avg_unit_price = ('unit_price','mean'),
    )
    .reset_index()
)

# ─────────────────────────────────────────────────────────────────────────────
# 7) 엑셀 저장
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    agg.to_excel(writer, sheet_name='연도별집계', index=False)

print(f"\n✅ 리포트가 '{output_file}' 로 저장되었습니다.")
