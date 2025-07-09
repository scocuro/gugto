#!/usr/bin/env python3
# monthly_price_index.py  (월별 매매가격 지수; STATBL_ID=A_2024_00178)

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
    code_df = pd.read_csv(CSV_PATH, encoding="euc-kr", skiprows=[0,1])
    print(f">>> DEBUG: 매핑 CSV 로드 성공 (rows={len(code_df)})")
except Exception as e:
    print(f"ERROR: 매핑 CSV 불러오기 실패 ({CSV_PATH}): {e}")
    sys.exit(1)

# 2) 시도 전체명 → 약어 매핑
sido_map = {
    "서울특별시":"서울","서울시":"서울","서울":"서울",
    "부산광역시":"부산","부산시":"부산","부산":"부산",
    "대구광역시":"대구","대구시":"대구","대구":"대구",
    "인천광역시":"인천","인천시":"인천","인천":"인천",
    "광주광역시":"광주","광주시":"광주",
    "대전광역시":"대전","대전시":"대전",
    "울산광역시":"울산","울산시":"울산",
    "세종특별자치시":"세종","세종시":"세종","세종":"세종",
    "경기도":"경기","경기":"경기",
    "강원도":"강원","강원":"강원",
    "충청북도":"충북","충북":"충북",
    "충청남도":"충남","충남":"충남",
    "전라북도":"전북","전북":"전북",
    "전라남도":"전남","전남":"전남",
    "경상북도":"경북","경북":"경북",
    "경상남도":"경남","경남":"경남",
    "제주특별자치도":"제주","제주도":"제주","제주":"제주",
}
def map_sido(full_name: str) -> str:
    if full_name in sido_map:
        return sido_map[full_name]
    for k,v in sido_map.items():
        if full_name.startswith(k):
            return v
    raise ValueError(f"알 수 없는 시도명: '{full_name}'")

# 3) 반환할 지역 라벨 목록 생성 (중복 제거 포함)
def get_region_labels(region_name: str) -> list:
    parts = region_name.split()
    sido = map_sido(parts[0])
    raw = ["전국","수도권","지방권","서울", sido]
    if len(parts) >= 2:
        raw.append(f"{sido} {parts[1]}")
    if len(parts) >= 3:
        raw.append(f"{sido} {parts[1]} {parts[2]}")
    # 중복 제거, 순서 유지
    seen = set()
    labels = []
    for lbl in raw:
        if lbl not in seen:
            seen.add(lbl)
            labels.append(lbl)
    return labels

# 4) 분류명→CLS_ID
def get_cls_id(label: str) -> str:
    sub = code_df[code_df['분류명'] == label]
    if sub.empty:
        raise LookupError(f"CSV에서 '{label}' 코드 미발견")
    cls = str(sub['지역코드'].iloc[0])
    print(f">>> DEBUG: label='{label}' → cls_id='{cls}'")
    return cls

# 5) CLI 파싱
parser = argparse.ArgumentParser(description="월별 매매가격 지수 보고서 생성기 (debug)")
parser.add_argument('--region-name', required=True, help='시도[ 시군구[ 읍면동]]')
parser.add_argument('--start',       required=True, help='조회 시작 시점 (YYYYMM)')
parser.add_argument('--end',         required=True, help='조회 종료 시점 (YYYYMM)')
parser.add_argument('--output',      default='price_index.xlsx', help='출력 엑셀 파일명')
args = parser.parse_args()
print(f">>> DEBUG: 입력 파라미터: region_name={args.region_name!r}, start={args.start!r}, end={args.end!r}")

# 6) API 키 확인
API_KEY = os.getenv('REB_API_KEY')
if not API_KEY:
    print("ERROR: REB_API_KEY 환경변수를 설정하세요."); sys.exit(1)

BASE_URL    = "https://www.reb.or.kr/r-one/openapi/SttsApiTblData.do"
STATBL_ID   = "A_2024_00178"
DTACYCLE_CD = "MM"
ITM_ID      = "100001"

# 7) 데이터 수집
labels = get_region_labels(args.region_name)
print(f">>> DEBUG: 생성된 지역 라벨: {labels!r}")

dfs = []
for lbl in labels:
    print(f">>> DEBUG: 수집 시작: '{lbl}'")
    try:
        cls_id = get_cls_id(lbl)
    except Exception as e:
        print(f">>> WARNING: {e}, 건너뜁니다."); continue

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
    print(f">>> DEBUG: 요청 params={params}")

    try:
        r = requests.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        print(f">>> DEBUG: 응답 상태={r.status_code}, preview={r.text[:200]!r}")
    except Exception as e:
        print(f">>> ERROR: 요청 실패 '{lbl}': {e}"); continue

    try:
        tmp = pd.read_xml(StringIO(r.text), xpath='.//row', parser='etree')
    except Exception as e:
        print(f">>> ERROR: XML 파싱 실패 '{lbl}': {e}"); continue

    if tmp.empty or 'WRTTIME_IDTFR_ID' not in tmp.columns or 'DTA_VAL' not in tmp.columns:
        print(f">>> DEBUG: 데이터 없음 '{lbl}', 건너뜁니다."); continue

    df = tmp[['WRTTIME_IDTFR_ID','DTA_VAL']].rename(
        columns={'WRTTIME_IDTFR_ID':'연월','DTA_VAL':lbl}
    )
    df[lbl] = pd.to_numeric(df[lbl], errors='coerce')
    print(f">>> DEBUG: parsed rows '{lbl}': {len(df)}")
    dfs.append(df)

if not dfs:
    print("ERROR: 수집된 데이터가 없습니다."); sys.exit(1)

# 8) 병합 & 연월 문자열 강제
print(f">>> DEBUG: 병합할 DF 수={len(dfs)}")
df_all = reduce(lambda a,b: pd.merge(a,b,on='연월',how='outer'), dfs)
df_all = df_all.sort_values('연월').reset_index(drop=True)
df_all['연월'] = df_all['연월'].astype(str).str.zfill(6)
print(f">>> DEBUG: df_all shape={df_all.shape}")

# (1) 전체 전치
df_trans = df_all.set_index('연월').T.reset_index().rename(columns={'index':'지역'})
print(f">>> DEBUG: df_transposed shape={df_trans.shape}")

# (2) 연말 및 최신 요약
sy = int(args.start[:4]); ey = int(args.end[:4])
last_m = args.end[4:]
months = []
for y in range(sy, ey+1):
    if y < ey:
        months.append(f"{y}12")
    else:
        months.append(f"{y}{last_m}")
print(f">>> DEBUG: summary months={months!r}")

df_sum = df_all[df_all['연월'].isin(months)].copy()
# 포맷 "YYYY.MM"
df_sum['연월'] = df_sum['연월'].apply(lambda x: f"{x[:4]}.{x[4:]}")
df_sum_t = df_sum.set_index('연월').T.reset_index().rename(columns={'index':'지역'})
print(f">>> DEBUG: df_summary_t shape={df_sum_t.shape}")

# 9) 엑셀 쓰기
with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    df_trans.to_excel(writer, sheet_name='매매가격지수', index=False)
    df_sum_t.to_excel(writer, sheet_name='연말 및 최신 요약', index=False)

print(f">>> DEBUG: 엑셀 작성 완료: {args.output!r}")
print(f"✅ '{args.output}'에 저장되었습니다.")
