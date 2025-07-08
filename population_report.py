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
    입력 예시:
      - '충청남도'
      - '경상남도 진주시'
      - '경기도 수원시 영통구'
    반환: 10자리 법정동코드 (예: '4817000000')
    """
    parts = region_name.split()
    sido = parts[0]
    if len(parts) == 1:
        sub = csv_df[csv_df["시도명"] == sido]
    elif len(parts) == 2:
        sigungu = parts[1]
        sub = csv_df[
            (csv_df["시도명"] == sido) &
            (csv_df["시군구명"] == sigungu) &
            (csv_df["읍면동명"].isna())
        ]
    elif len(parts) == 3:
        full_sigungu = parts[1] + parts[2]
        sub = csv_df[
            (csv_df["시도명"] == sido) &
            (csv_df["시군구명"] == full_sigungu)
        ]
    else:
        raise ValueError("‘시도’, ‘시도 시군구’, ‘시도 시군구 읍면동’ 형식만 지원합니다.")
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")
    # 법정동코드 전체 10자리
    code10 = sub.iloc[0]["법정동코드"]
    if len(code10) != 10:
        raise ValueError(f"법정동코드 길이가 10자리가 아닙니다: {code10}")
    return code10

# ── 2) CLI 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 인구·세대수 리포트 생성기")
parser.add_argument('--region-name', required=True,
                    help='시도[ 시군구[ 읍면동]] (예: 경상남도 진주시)')
parser.add_argument('--start', required=True,
                    help='조회 시작년월 (YYYYMM)')
parser.add_argument('--end',   required=True,
                    help='조회 종료년월 (YYYYMM), 시작으로부터 최대 3개월 이내여야 함')
parser.add_argument('--lv',     type=int, default=2,
                    help='조회레벨: 1=광역시도,2=시군구,3=읍면동,4=읍면동통합')
parser.add_argument('--regSeCd', type=int, default=1,
                    help='등록구분: 1=전체,2=거주자,3=거주불명자,4=재외국민')
parser.add_argument('--output', default='population_report.xlsx',
                    help='출력 엑셀 파일명')
args = parser.parse_args()

# ── 3) 시군구코드 결정 ──
try:
    admmCd = get_admmCd(args.region_name)
except Exception as e:
    print("ERROR:", e)
    sys.exit(1)

# ── 4) API 키 & 엔드포인트 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)

BASE_POP_URL = "http://apis.data.go.kr/1741000/admmPpltnHhStus/selectAdmPpltnHhStus"

# ── 5) 단일 페이지 호출 ──
def fetch_population_page(srchFrYm, srchToYm, page):
    params = {
        'serviceKey': API_KEY,
        'admmCd':     admmCd,                # ← 수정: admmCd (m m)
        'srchFrYm':   srchFrYm,
        'srchToYm':   srchToYm,
        'lv':         args.lv,
        'regSeCd':    args.regSeCd,
        'type':       'JSON',               # ← JSON 요청
        'numOfRows':  100,
        'pageNo':     page,
    }
    r = requests.get(BASE_POP_URL, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()
    head = js.get('response', {}).get('header', {})
    # resultCode가 00이 아닐 경우 데이터 없음
    if head.get('resultCode') != '00':
        return []
    body = js['response']['body']
    items = body.get('items', {}).get('item', [])
    if not items:
        return []
    return items if isinstance(items, list) else [items]

# ── 6) 조회 기간을 3개월 단위로 분할 ──
def split_to_quarters(start_yyyymm, end_yyyymm):
    """start≤end 사이를 3개월 간격의 (fr,to) 튜플 리스트로 분할."""
    def to_ym(s):
        y = int(s[:4]); m = int(s[4:6])
        return y, m
    def to_str(y, m):
        return f"{y}{m:02d}"
    y0, m0 = to_ym(start_yyyymm)
    ye, me = to_ym(end_yyyymm)
    chunks = []
    cur_y, cur_m = y0, m0
    while (cur_y, cur_m) <= (ye, me):
        # 2개월 더해 3개월 범위 완료 시점 계산
        total_month = cur_y*12 + (cur_m-1) + 2
        ey = total_month // 12
        em = (total_month % 12) + 1
        # 잘라낼 종료 = min(ey, em, 실제 종료)
        if (ey, em) > (ye, me):
            ey, em = ye, me
        chunks.append((to_str(cur_y, cur_m), to_str(ey, em)))
        # 다음 시작 = ey, em + 1개월
        nxt_total = ey*12 + (em-1) + 1
        cur_y = nxt_total // 12
        cur_m = (nxt_total % 12) + 1
    return chunks

# ── 7) 전체 수집 ──
def collect_population():
    all_rows = []
    for fr, to in split_to_quarters(args.start, args.end):
        page = 1
        while True:
            try:
                items = fetch_population_page(fr, to, page)
            except Exception as e:
                print(f"WARNING: 페이지 {page} 호출 실패: {e}")
                break
            if not items:
                break
            all_rows.extend(items)
            page += 1
    return pd.DataFrame(all_rows)

# ── 8) 데이터 수집 실행 ──
print(f"▶ 인구·세대 데이터 수집: {args.start} → {args.end} (lv={args.lv})")
df_pop = collect_population()
print(f"  → {len(df_pop)}건 수집 완료")

# ── 9) 엑셀 저장 ──
if df_pop.empty:
    print("⚠️ 조회된 데이터가 없습니다. 기간과 레벨을 확인하세요.")
else:
    # 날짜순 정렬
    if 'srchToYm' in df_pop.columns:
        df_pop = df_pop.sort_values('srchToYm')
    with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
        df_pop.to_excel(writer, sheet_name='인구_세대(raw)', index=False)
    print(f"✅ '{args.output}' 에 저장되었습니다.")
