#!/usr/bin/env python3
# monthly_price_index.py  (월별 매매가격 지수; STATBL_ID=A_2024_00178) with debug logging

import argparse
import os
import sys
import requests
import pandas as pd
from io import StringIO
from functools import reduce

# 1) 지역코드 매핑 CSV 로드
CSV_PATH = "code_forprice.csv"
try:
    code_df = pd.read_csv(CSV_PATH, encoding="euc-kr", skiprows=[0,1], dtype=str)
    print(f">>> DEBUG: 매핑 CSV 로드 성공 (rows={len(code_df)})")
except Exception as e:
    print(f"ERROR: 매핑 CSV 불러오기 실패 ({CSV_PATH}): {e}")
    sys.exit(1)

# 2) 시도 전체명 → 약어 매핑
sido_map = {
    "서울특별시": "서울", "서울시": "서울", "서울": "서울",
    "부산광역시": "부산", "부산시": "부산", "부산": "부산",
    "대구광역시": "대구", "대구시": "대구", "대구": "대구",
    "인천광역시": "인천", "인천시": "인천", "인천": "인천",
    "광주광역시": "광주", "광주시": "광주",
    "대전광역시": "대전", "대전시": "대전",
    "울산광역시": "울산", "울산시": "울산",
    "세종특별자치시": "세종", "세종시": "세종", "세종": "세종",
    "경기도": "경기", "경기": "경기",
    "강원도": "강원", "강원": "강원",
    "충청북도": "충북", "충북": "충북",
    "충청남도": "충남", "충남": "충남",
    "전라북도": "전북", "전북": "전북",
    "전라남도": "전남", "전남": "전남",
    "경상북도": "경북", "경북": "경북",
    "경상남도": "경남", "경남": "경남",
    "제주특별자치도": "제주", "제주도": "제주", "제주": "제주",
}

def map_sido(full_name: str) -> str:
    if full_name in sido_map:
        return sido_map[full_name]
    for k, v in sido_map.items():
        if full_name.startswith(k):
            return v
    raise ValueError(f"알 수 없는 시도명: '{full_name}'")

# 3) 반환할 지역 라벨 목록 생성
# (전국, 수도권, 지방권, 시도, 시도+시군구, 시도+시군구+읍면동)
def get_region_labels(region_name: str) -> list:
    parts = region_name.split()
    sido_full = parts[0]
    sido = map_sido(sido_full)
    labels = ["전국", "수도권", "지방권", sido]
    if len(parts) >= 2:
        labels.append(f"{sido} {parts[1]}")
    if len(parts) >= 3:
        labels.append(f"{sido} {parts[1]} {parts[2]}")
    print(f">>> DEBUG: 생성된 지역 라벨 목록: {labels}")
    return labels

# 4) 레이블로부터 CLS_ID 추출 (GRP_ID 제거)
def get_code(label: str) -> str:
    sub = code_df[code_df['분류명'] == label]
    if sub.empty:
        raise LookupError(f"CSV에서 '{label}' 코드 미발견")
    cls_id = str(sub['법정동코드'].iloc[0])
    print(f">>> DEBUG: label='{label}' → cls_id='{cls_id}'")
    return cls_id

# 5) CLI 파싱
parser = argparse.ArgumentParser(description="월별 매매가격 지수 보고서 생성기")
parser.add_argument('--region-name', required=True, help='시도[ 시군구[ 읍면동]]')
parser.add_argument('--start',       required=True, help='조회 시작 시점 (YYYYMM)')
parser.add_argument('--end',         required=True, help='조회 종료 시점 (YYYYMM)')
parser.add_argument('--output',      default='price_index.xlsx', help='출력 엑셀 파일명')
args = parser.parse_args()
print(f">>> DEBUG: 입력 파라미터 region_name={args.region_name}, start={args.start}, end={args.end}, output={args.output}")

# 6) API 키 확인
API_KEY = os.getenv('REB_API_KEY')
if not API_KEY:
    print("ERROR: REB_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)

BASE_URL    = "https://www.reb.or.kr/r-one/openapi/SttsApiTblData.do"
STATBL_ID   = "A_2024_00178"
DTACYCLE_CD = "MM"
ITM_ID      = "100001"

# 7) 데이터 수집
dfs = []
region_labels = get_region_labels(args.region_name)
for label in region_labels:
    print(f">>> DEBUG: 수집 시작: '{label}'")
    try:
        cls_id = get_code(label)
    except Exception as e:
        print(f">>> WARNING: {e}, 건너뜁니다.")
        continue
    params = {
        'KEY':           API_KEY,
        'Type':          'xml',
        'pIndex':        1,
        'pSize':         1000,
        'STATBL_ID':     STATBL_ID,
        'DTACYCLE_CD':   DTACYCLE_CD,
        'CLS_ID':        cls_id,
        'ITM_ID':        ITM_ID,
        'START_WRTTIME': args.start,
        'END_WRTTIME':   args.end,
    }
    print(f">>> DEBUG: 요청 URL={BASE_URL}\n    params={params}")
    try:
        resp = requests.get(BASE_URL, params=params, timeout=30)
        print(f">>> DEBUG: 응답 URL: {resp.url}")
        resp.raise_for_status()
        print(f">>> DEBUG: 응답 상태 코드: {resp.status_code}, content-type={resp.headers.get('Content-Type')}")
        preview = resp.text[:200].replace('\n', ' ')
        print(f">>> DEBUG: 응답 텍스트 프리뷰: {preview!r}")
        temp = pd.read_xml(StringIO(resp.text), xpath='.//list', parser='etree')
        print(f">>> DEBUG: 파싱 후 레코드 수: {len(temp)}")
    except Exception as e:
        print(f">>> ERROR: '{label}' 조회 실패: {e}")
        continue
    if 'STATS_DATA' not in temp.columns:
        print(f">>> DEBUG: 'STATS_DATA' 컬럼 미발견, 건너뜁니다.")
        continue
    tmp = temp[['WRTTIME','STATS_DATA']].rename(columns={'WRTTIME':'연월', 'STATS_DATA': label})
    dfs.append(tmp)
    print(f">>> DEBUG: '{label}' 데이터프레임 shape: {tmp.shape}")

if not dfs:
    print("ERROR: 수집된 데이터가 없습니다.")
    sys.exit(1)

# 8) 병합 및 정렬
print(f">>> DEBUG: 데이터 병합 및 정렬 시작")
df_all = reduce(lambda left, right: pd.merge(left, right, on='연월', how='outer'), dfs)
df_all = df_all.sort_values('연월')
print(f">>> DEBUG: 병합 후 전체 shape: {df_all.shape}")

# 9) 엑셀 저장
print(f">>> DEBUG: 엑셀 파일 저장: {args.output}")
with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    df_all.to_excel(writer, sheet_name='매매가격지수', index=False)

print(f"✅ '{args.output}'에 저장되었습니다.")
