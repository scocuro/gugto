#!/usr/bin/env python3
# population_report.py

import argparse
import os
import sys
import requests
import pandas as pd

# ── 1) 시군구 코드 CSV 로드 ──
CSV_PATH = "code_raw.csv"
try:
    csv_df = pd.read_csv(CSV_PATH, encoding="utf-8", dtype=str)
except Exception as e:
    print(f"ERROR: 시군구 코드 CSV를 불러오는 데 실패했습니다 ({CSV_PATH}): {e}")
    sys.exit(1)

def get_region_code(region_name: str) -> str:
    """'충청남도 천안시 동남구' 같은 이름으로부터
       법정동코드 상위 5자리(시군구) 또는 7~10자리(읍면동) 코드를 돌려줍니다."""
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
        # 읍면동까지 주실 경우 full_sigungu = 시군구 + 읍면동
        full_sigungu = parts[1] + parts[2]
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == full_sigungu)
        ]
    else:
        raise ValueError("‘시도 시군구’ 또는 ‘시도 시군구 읍면동’ 형식으로 입력해 주세요.")
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")
    # 5자리(시군구)만 쓰려면 [:5], 읍면동까지 쓰려면 [:10]
    return sub.iloc[0]["법정동코드"][:5]

# ── 2) CLI 옵션 파싱 ──
parser = argparse.ArgumentParser(description="인구·세대수 리포트 생성기")
grp = parser.add_mutually_exclusive_group(required=True)
grp.add_argument('--lawd-cd',     help='5자리 시군구코드를 직접 입력')
grp.add_argument('--region-name', help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
parser.add_argument('--start', required=True, help='조회 시작 YYYYMM (예: 202101)')
parser.add_argument('--end',   required=True, help='조회 종료 YYYYMM (예: 202312)')
parser.add_argument('--output', default='population_report.xlsx', help='출력 엑셀 파일명')
args = parser.parse_args()

# ── 3) 지역코드 결정 ──
if args.lawd_cd:
    region_code = args.lawd_cd
else:
    try:
        region_code = get_region_code(args.region_name)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)

# ── 4) 환경변수로부터 서비스키 ──
SERVICE_KEY = os.getenv('SERVICE_POP_KEY')
if not SERVICE_KEY:
    print("ERROR: SERVICE_POP_KEY 환경변수를 설정하세요.")
    sys.exit(1)

# ── 5) API 엔드포인트 ──
POP_URL = "http://apis.data.go.kr/1741000/admmPpltnHhStus/selectAdmPpltnHhStus"

# ── 6) 페이징 처리해서 JSON → 리스트 반환 헬퍼 ──
def fetch_population_page(params):
    """한 페이지만 가져와서 item 리스트로 반환"""
    r = requests.get(POP_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    # 표준 data.go.kr JSON 구조에서 실제 항목들:
    items = (data
             .get('response', {})
             .get('body', {})
             .get('items', {})
             .get('item', []))
    return items or []

# ── 7) 전체 기간 조회 함수 ──
def collect_population(start_ym: str, end_ym: str, level: int):
    """start_ym~end_ym 사이를 페이징 돌며 전부 수집"""
    all_records = []
    page = 1
    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            'admnmCd':    region_code,
            'srchFrYm':   start_ym,
            'srchToYm':   end_ym,
            'lv':         level,      # 1=광역시도,2=시군구,3=읍면동
            'regSeCd':    '1',        # 전체(1), 거주자(2) 등
            'type':       'json',
            'numOfRows':  '1000',
            'pageNo':     str(page)
        }
        items = fetch_population_page(params)
        if not items:
            break
        all_records.extend(items)
        page += 1

    return pd.DataFrame(all_records)

# ── 8) level 결정 ──
# region-name 스플릿 길이대로 lv 설정
if args.lawd_cd:
    lv = 2
else:
    parts = args.region_name.split()
    if len(parts) == 1:
        lv = 1
    elif len(parts) == 2:
        lv = 2
    else:
        lv = 3

# ── 9) 수집 및 엑셀 쓰기 ──
print(f"▶ 인구·세대 데이터 수집 (lv={lv})…")
df_pop = collect_population(args.start, args.end, lvl := lv)
print(f"  → {len(df_pop)}건 수집 완료")

# 예: 주요칼럼만 정렬해서 출력
cols = [c for c in ['sidoNm','sggNm','emdNm','date','totalPpltn','householdCount'] if c in df_pop.columns]
with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    df_pop.to_excel(writer, sheet_name='인구세대(raw)', index=False, columns=cols)

print(f"✅ '{args.output}'에 저장되었습니다.")
