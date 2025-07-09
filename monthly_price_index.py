#!/usr/bin/env python3
# monthly_price_index.py

import argparse
import os
import sys
import requests
import pandas as pd
from datetime import datetime
from io import StringIO

# ── 1) 지역 코드 CSV 로드 ──
CSV_PATH = "code_forprice.csv"
try:
    # 1,2번째 헤더 행을 스킵하고, 분류명·지역코드·법정동코드 컬럼만 읽어옴
    code_df = pd.read_csv(CSV_PATH, encoding="euc-kr", skiprows=2, dtype=str)
    code_df.columns = ["분류명", "지역코드", "법정동코드"]
except Exception as e:
    print(f"ERROR: 지역 코드 파일을 불러오는 데 실패했습니다 ({CSV_PATH}): {e}")
    sys.exit(1)

# ── 2) 시도명 축약 매핑 ──
abbr_map = {
    "서울특별시": "서울",  "부산광역시": "부산",  "대구광역시": "대구",
    "인천광역시": "인천",  "광주광역시": "광주",  "대전광역시": "대전",
    "울산광역시": "울산",  "세종특별자치시": "세종",
    "경기도":   "경기",  "강원도":   "강원",
    "충청북도": "충북",  "충청남도": "충남",
    "전라북도": "전북",  "전라남도": "전남",
    "경상북도": "경북",  "경상남도": "경남",
    "제주특별자치도": "제주",
}

def normalize_name(parts):
    # 시도명 축약
    parts[0] = abbr_map.get(parts[0], parts[0])
    return parts

def build_region_list(region_name: str):
    parts = region_name.strip().split()
    parts = normalize_name(parts)
    # 항상 반환할 3개 상위 지역
    regions = ["전국", "수도권", "지방권"]
    # 입력된 수준별로 추가
    if len(parts) >= 1:
        regions.append(parts[0])  # ex. "충남"
    if len(parts) >= 2:
        regions.append(f"{parts[0]} {parts[1]}")  # ex. "충남 천안시"
    if len(parts) >= 3:
        regions.append(f"{parts[0]} {parts[1]} {parts[2]}")  # ex. "충남 천안시 동남구"

    result = []
    for reg in regions:
        sub = code_df[code_df["분류명"] == reg]
        if sub.empty:
            print(f"ERROR: '{reg}'에 대한 코드가 없습니다.")
        else:
            code = sub.iloc[0]["지역코드"]
            result.append((reg, code))
    if not result:
        print("ERROR: 유효한 지역 코드가 하나도 없습니다.")
        sys.exit(1)
    return result

# ── 3) CLI 파싱 ──
parser = argparse.ArgumentParser(description="월별 매매가격지수 수집기")
parser.add_argument('--region-name', required=True,
                    help='시도[ 시군구[ 읍면동]] (예: 충청남도 천안시 동남구)')
parser.add_argument('--start', required=True,
                    help='시작기간 (YYYYMM)')
parser.add_argument('--end', required=True,
                    help='종료기간 (YYYYMM)')
parser.add_argument('--statbl-id', default='A_2024_00178',
                    help='통계표 ID (STATBL_ID)')
parser.add_argument('--output', default='monthly_price_index.xlsx',
                    help='출력 엑셀 파일명')
args = parser.parse_args()

# ── 4) API 키 & 엔드포인트 ──
API_KEY = os.getenv('REB_KEY')
if not API_KEY:
    print("ERROR: REB_KEY 환경변수를 설정하세요.")
    sys.exit(1)

BASE_URL = "https://kosis.kr/openapi/statisticsData.do"

# ── 5) 데이터 조회 헬퍼 ──
def fetch_index(region_code: str) -> pd.DataFrame:
    params = {
        'method':    'getList',
        'apiKey':    API_KEY,
        'format':    'xml',
        'statCode':  args.statbl_id,
        'startTime': args.start,
        'endTime':   args.end,
        'cycle':     'M',            # 월별
        'region':    region_code,    # 지역 코드
    }
    try:
        r = requests.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"ERROR: '{region_code}' 조회 실패: {e}")
        return pd.DataFrame()
    try:
        # 실제 API 결과의 item 태그 경로에 맞춰 조정하세요
        df = pd.read_xml(StringIO(r.text), xpath='.//item')
    except Exception as e:
        print(f"ERROR: '{region_code}' XML 파싱 실패: {e}")
        return pd.DataFrame()
    return df

# ── 6) 전 지역 수집 ──
region_list = build_region_list(args.region_name)
all_dfs = []
for reg_name, reg_code in region_list:
    print(f"▶ '{reg_name}' 수집...")
    df = fetch_index(reg_code)
    if df.empty:
        continue
    # 기간 컬럼과 값 컬럼명은 API 스펙에 맞춰 조정
    df.rename(columns={'TIME': '기간', 'VALUE': '지수'}, inplace=True)
    df['지역'] = reg_name
    all_dfs.append(df)

if not all_dfs:
    print("ERROR: 수집된 데이터가 없습니다.")
    sys.exit(1)

df_all = pd.concat(all_dfs, ignore_index=True)
# '기간'이 YYYYMM 형태라고 가정
df_all['년도'] = df_all['기간'].astype(str).str[:4].astype(int)

# ── 7) 피벗 테이블 생성 ──
pivot = (
    df_all
    .pivot_table(index='년도', columns='지역', values='지수', aggfunc='mean')
    .sort_index()
)

# ── 8) 엑셀 출력 ──
with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    df_all.to_excel(writer, sheet_name='raw', index=False)
    pivot.to_excel(writer, sheet_name='pivot')

print(f"✅ '{args.output}' 에 저장되었습니다.")
