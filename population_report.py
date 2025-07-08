#!/usr/bin/env python3
# population_report.py

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
    """
    region_name: '경기도 수원시 영통구' 등
    CSV의 '법정동코드'(10자리) 전체를 반환합니다.
    """
    parts = region_name.split()
    sido = parts[0]
    if len(parts) == 2:
        sigungu = parts[1]
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == sigungu) &
            (csv_df["읍면동명"].isna())
        ]
    elif len(parts) == 3:
        full_sigungu = parts[1] + parts[2]
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == full_sigungu)
        ]
    else:
        raise ValueError("‘시도 시군구’ 또는 ‘시도 시군구 읍면동’ 형식으로 입력해 주세요.")
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")
    # 법정동코드 전체(10자리)를 반환
    return sub.iloc[0]["법정동코드"]

def determine_admn_cd(full_code: str, lv: int) -> str:
    """
    lv = 1 (시도 단위) → 앞 2자리 + '00000000'
    lv = 2 (시군구 단위) → 앞 5자리 + '00000'
    lv = 3 (읍면동 단위) → full 10자리 그대로
    """
    if lv == 1:
        return full_code[:2] + "00000000"
    elif lv == 2:
        return full_code[:5] + "00000"
    else:
        return full_code  # 이미 10자리

# ── 2) CLI 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 인구·세대 리포트 생성기")
parser.add_argument('--region-name',
                    help='시도+시군구(+읍면동) 명칭 (예: 경상남도 진주시 또는 경기도 수원시 영통구)',
                    required=True)
parser.add_argument('--start', help='조회 시작 기간 (YYYYMM)', required=True)
parser.add_argument('--end',   help='조회 종료 기간 (YYYYMM)', required=True)
parser.add_argument('--output', help='출력 엑셀 파일명', default='population_report.xlsx')
args = parser.parse_args()

# ── 3) admnCd 결정 ──
try:
    full_code = get_region_code(args.region_name)
except Exception as e:
    print("ERROR:", e)
    sys.exit(1)

parts = args.region_name.split()
lv = len(parts)  # 1=시도, 2=시군구, 3=읍면동
admn_cd = determine_admn_cd(full_code, lv)
print(f"▶ 인구·세대 데이터 수집 (lv={lv}, admnCd={admn_cd})…")

# ── 4) API 키 & 엔드포인트 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)

BASE_POP_URL = "http://apis.data.go.kr/1741000/admmPpltnHhStus/selectAdmPpltnHhStus"

# ── 5) 단일 페이지 Fetch 헬퍼 ──
def fetch_population_page(params: dict) -> list[dict]:
    try:
        r = requests.get(BASE_POP_URL, params=params, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"WARNING: 페이지 {params.get('pageNo')} 호출 실패: {e}")
        return []
    # JSON 응답 파싱 (구조에 따라 조정 필요)
    j = r.json()
    # 예: j['response']['body']['items']['item']
    items = j.get('response', {}) \
             .get('body', {}) \
             .get('items', {}) \
             .get('item', [])
    return items

# ── 6) 전체 수집 ──
def collect_population(start: str, end: str, lv: int) -> pd.DataFrame:
    rows = []
    page = 1
    while True:
        params = {
            'serviceKey': API_KEY,
            'admnCd':     admn_cd,
            'srchFrYm':   start,
            'srchToYm':   end,
            'lv':         lv,
            'regSeCd':    '1',      # 전체인구+세대: 1
            'type':       'JSON',
            'numOfRows':  1000,
            'pageNo':     page,
        }
        recs = fetch_population_page(params)
        if not recs:
            break
        rows.extend(recs)
        page += 1

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)

# ── 7) 실제 수집 & 엑셀 저장 ──
print("▶ 데이터 수집 중…")
df_pop = collect_population(args.start, args.end, lv)
print(f"  → {len(df_pop)}건 수집 완료")

with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    df_pop.to_excel(writer, sheet_name='인구세대(raw)', index=False)

print(f"✅ '{args.output}' 에 저장되었습니다.")
