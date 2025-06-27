# real_estate_report.py

import argparse
import os
import sys
import requests
import pandas as pd
from datetime import datetime

# ── 1) 시군구 코드 CSV 로드 ──
#    사용하시는 CSV 파일 경로로 수정하세요.
CSV_PATH = "code_raw.csv"

try:
    csv_df = pd.read_csv(CSV_PATH, encoding="utf-8", dtype=str)
except Exception as e:
    print(f"ERROR: 시군구 코드 CSV를 불러오는 데 실패했습니다 ({CSV_PATH}): {e}")
    sys.exit(1)

def get_region_code(region_name: str) -> str:
    """
    '서울특별시 종로구' 또는 '충청남도 천안시 동남구' 형태의 이름을 받아
    해당하는 시군구코드(앞 5자리)만 반환합니다.
    """
    parts = region_name.split()
    sido = parts[0]  # ex. "서울특별시", "충청남도"
    
    # 시도 + 시군구
    if len(parts) == 2:
        sigungu = parts[1]  # ex. "종로구"
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == sigungu) &
            (csv_df["읍면동명"].isna())
        ]
    # 시도 + 시군구 + 읍면동(필요시)
    elif len(parts) == 3:
        city, gu = parts[1], parts[2]
        full_sigungu = city + gu  # ex. "천안시" + "동남구"
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == full_sigungu)
        ]
    else:
        raise ValueError("‘시도 종군구’ 또는 ‘시도 시군구 읍면동’ 형식으로 입력해 주세요.")
    
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")
    
    full_code = sub.iloc[0]["법정동코드"]  # 10자리 코드
    return full_code[:5]                  # 앞 5자리만 리턴

# ── 2) 커맨드라인 인자 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--lawd-cd',      help='직접 사용할 법정동코드(앞 5자리 시군구코드)')
group.add_argument('--region-name',  help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
parser.add_argument('--start-year',   type=int, default=2020, help='조회 시작 연도(YYYY)')
parser.add_argument('--built-after',  type=int, default=2015, help='준공 연도 이후 필터')
parser.add_argument('--output',       default='report.xlsx', help='출력 엑셀 파일명')
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

start_year  = args.start_year
built_after = args.built_after
output_file = args.output

# ── 4) 거래 API 엔드포인트 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: 환경변수 PUBLIC_DATA_API_KEY에 API 키를 설정하세요.")
    sys.exit(1)

print(f"DEBUG: API_KEY length = {len(API_KEY)}")

BASE_URL = (
    "http://apis.data.go.kr/1613000/"
    "RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
)

def fetch_transactions(lawd_cd: str, deal_ym: str, page_no: int) -> pd.DataFrame:
    page_size = 1000
    params = {
        'serviceKey': API_KEY,
        'LAWD_CD':    lawd_cd,
        'DEAL_YMD':   deal_ym,
        'pageNo':     page_no,
        'numOfRows':  page_size,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    # 내장 etree 파서 사용
    df = pd.read_xml(resp.content, xpath='.//item', parser='lxml')
    return df

# ── 5) 전체 기간 데이터 수집 ──
records = []
current_year = datetime.now().year
for year in range(start_year, current_year + 1):
    for month in range(1, 13):
        deal_ym = f"{year}{month:02d}"
        page = 1
        while True:
            try:
                df = fetch_transactions(region_code, deal_ym, page)
            except Exception as e:
                print(f"WARNING: {deal_ym} 페이지 {page} 호출 실패: {e}")
                break
            if df.empty:
                break
            # 건축년도 필터
            if '건축년도' in df.columns:
                df = df[df['건축년도'].astype(int) >= built_after]
            if df.empty or len(df) < page_size:
                break
            records.append(df)
            page += 1

if not records:
    print("조건에 맞는 거래 데이터가 없습니다.")
    sys.exit(0)

# ── 6) 병합 및 집계 ──
all_data = pd.concat(records, ignore_index=True)
if '년월' in all_data.columns:
    all_data['거래년월'] = pd.to_datetime(all_data['년월'], format='%Y%m')
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
        avg_price=('거래금액','mean'),
        count=('거래금액','size'),
        avg_unit_price=('unit_price','mean')
    )
    .reset_index()
)

# ── 7) 엑셀로 저장 ──
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    agg.to_excel(writer, sheet_name='연도별집계', index=False)

print(f"리포트가 '{output_file}' 로 저장되었습니다.")
