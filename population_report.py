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

def get_admmCd(region_name: str) -> str:
    """
    '충청남도' 혹은 '경기도 수원시' 혹은 '경기도 수원시 영통구' 형식 입력 → 
    행정구역코드(10자리) 반환
    """
    parts = region_name.split()
    sido = parts[0]
    if len(parts) == 1:
        # 시도만
        sub = csv_df[
            (csv_df["시도명"] == sido) &
            (csv_df["시군구명"].isna()) &
            (csv_df["읍면동명"].isna())
        ]
    elif len(parts) == 2:
        # 시도+시군구
        sigungu = parts[1]
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == sigungu) &
            (csv_df["읍면동명"].isna())
        ]
    elif len(parts) == 3:
        # 시도+시군구+읍면동
        full_sigungu = parts[1]
        emd = parts[2]
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == full_sigungu) &
            (csv_df["읍면동명"] == emd)
        ]
    else:
        raise ValueError("지원되는 형식: '시도', '시도 시군구', '시도 시군구 읍면동'")

    if sub.empty:
        raise LookupError(f"코드를 찾을 수 없습니다: '{region_name}'")
    code10 = sub.iloc[0]["법정동코드"]
    if len(code10) != 10:
        raise ValueError(f"코드 길이가 10자리가 아닙니다: {code10}")
    return code10

# ── 2) CLI 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 인구·세대수 리포트")
parser.add_argument('--region-name', required=True,
                    help="예: '경기도', '경기도 수원시', '경기도 수원시 영통구'")
parser.add_argument('--start', required=True, help="조회 시작년월 (YYYYMM)")
parser.add_argument('--end',   required=True, help="조회 종료년월 (YYYYMM)")
parser.add_argument('--regSeCd', type=int, default=1,
                    help="등록구분: 1=전체,2=거주자,3=거주불명자,4=재외국민")
parser.add_argument('--output', default='population_report.xlsx',
                    help='출력 엑셀파일명')
args = parser.parse_args()

# ── 3) 호출할 (lv, admmCd) 목록 준비 ──
parts = args.region_name.split()
sido = parts[0]
calls = []

# lv=1 (시도)
prov_code = get_admmCd(sido)
calls.append((1, prov_code))

# lv=2 (시도+시군구) — parts>=2 일 때만
if len(parts) >= 2:
    city_name = parts[1]
    city_code = get_admmCd(f"{sido} {city_name}")
    calls.append((2, city_code))

# lv=3 (읍면동) — parts==3 일 때만
if len(parts) == 3:
    dong_name = parts[2]
    dong_code = get_admmCd(args.region_name)
    calls.append((3, dong_code))

# ── 4) API 키 & 엔드포인트 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)

BASE_URL = "http://apis.data.go.kr/1741000/admmPpltnHhStus/selectAdmmPpltnHhStus"

# ── 5) 단일 페이지 fetch ──
def fetch_page(lv, admmCd, fr, to, page):
    params = {
        'serviceKey': API_KEY,
        'admmCd':     admmCd,
        'srchFrYm':   fr,
        'srchToYm':   to,
        'lv':         lv,
        'regSeCd':    args.regSeCd,
        'type':       'JSON',
        'numOfRows':  100,
        'pageNo':     page,
    }
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()
    resp = js.get('Response', {})
    head = resp.get('head', {})
    # 성공코드는 문자열 '0'
    if head.get('resultCode') != '0':
        return []
    items = resp.get('items', {}).get('item', [])
    if not items:
        return []
    return items if isinstance(items, list) else [items]

# ── 6) 3개월 단위 분할 ──
def split_to_quarters(start, end):
    def to_ym(s): return int(s[:4]), int(s[4:6])
    def to_str(y,m): return f"{y}{m:02d}"
    y0,m0 = to_ym(start)
    ye,me = to_ym(end)
    out = []
    cy, cm = y0, m0
    while (cy,cm) <= (ye,me):
        total = cy*12 + (cm-1) + 2
        ey, em = total//12, (total%12)+1
        if (ey,em) > (ye,me):
            ey,em = ye,me
        out.append((to_str(cy,cm), to_str(ey,em)))
        nxt = ey*12 + (em-1) + 1
        cy, cm = nxt//12, (nxt%12)+1
    return out

# ── 7) 전 구간 수집 ──
all_items = []
for lv, code in calls:
    for fr, to in split_to_quarters(args.start, args.end):
        page = 1
        print(f"▶ lv={lv}, admmCd={code}, 기간 {fr}→{to} …")
        while True:
            page_items = fetch_page(lv, code, fr, to, page)
            if not page_items:
                break
            # 같은 구조지만, lv=3일 땐 'emdNm'이 들어있음
            all_items.extend(page_items)
            page += 1

df_raw = pd.DataFrame(all_items)
print(f"  → 총 {len(df_raw)}건 raw 수집 완료")

# ── 8) 필요한 컬럼 뽑아서 한글로 리네임 ──
# emdNm이 있으면 그걸, 없으면 sggNm을 “시군구”로
df = pd.DataFrame({
    "시점": df_raw["]()_
