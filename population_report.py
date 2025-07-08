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
        raise ValueError("‘시도’, ‘시도 시군구’, ‘시도 시군구 읍면동’ 형식만 지원합니다.")
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")
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
                    help='조회 종료년월 (YYYYMM), 시작으로부터 최대 3개월 단위 호출')
parser.add_argument('--regSeCd', type=int, default=1,
                    help='등록구분: 1=전체,2=거주자,3=거주불명자,4=재외국민')
parser.add_argument('--output', default='population_report.xlsx',
                    help='출력 엑셀 파일명')
args = parser.parse_args()

# ── 3) 10자리 코드 추출 & 호출할 (lv,admmCd) 리스트 준비 ──
try:
    code10 = get_admmCd(args.region_name)
except Exception as e:
    print("ERROR:", e)
    sys.exit(1)

parts = args.region_name.split()
calls = []
# lv=1: 시도 (code10[0:2] + '00000000')
calls.append((1, code10[:2] + "00000000"))
if len(parts) >= 2:
    # lv=2: 시군구 (code10[0:5] + '00000')
    calls.append((2, code10[:5] + "00000"))
if len(parts) == 3:
    # lv=3: 읍면동 (full code)
    calls.append((3, code10))

# ── 4) API 키 & 엔드포인트 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)
BASE_POP_URL = "http://apis.data.go.kr/1741000/admmPpltnHhStus/selectAdmmPpltnHhStus"

# ── 5) 단일 페이지 호출 ──
def fetch_population_page(lv, admmCd, fr, to, page):
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
    r = requests.get(BASE_POP_URL, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()
    resp = js.get('Response', {})
    head = resp.get('head', {})
    if head.get('resultCode') != '0':
        return []
    items = resp.get('items', {}).get('item', [])
    if not items:
        return []
    return items if isinstance(items, list) else [items]

# ── 6) 3개월 단위 분할 ──
def split_to_quarters(start_yyyymm, end_yyyymm):
    def to_ym(s):
        return int(s[:4]), int(s[4:6])
    def to_str(y,m):
        return f"{y}{m:02d}"
    y0,m0 = to_ym(start_yyyymm)
    ye,me = to_ym(end_yyyymm)
    chunks = []
    cy,cm = y0,m0
    while (cy,cm) <= (ye,me):
        total = cy*12 + (cm-1) + 2
        ey,em = total//12, (total%12)+1
        if (ey,em) > (ye,me):
            ey,em = ye,me
        chunks.append((to_str(cy,cm), to_str(ey,em)))
        nxt = ey*12 + (em-1) + 1
        cy,cm = nxt//12, (nxt%12)+1
    return chunks

# ── 7) 전체 수집 ──
all_rows = []
for lv, admm in calls:
    for fr, to in split_to_quarters(args.start, args.end):
        page = 1
        print(f"▶ lv={lv}, admmCd={admm} 기간 {fr}→{to} 수집…")
        while True:
            items = fetch_population_page(lv, admm, fr, to, page)
            if not items:
                break
            # 각 item에 현재 lv와 admmCd 정보는 필요 없으므로 그냥 수집
            all_rows.extend(items)
            page += 1

df = pd.DataFrame(all_rows)
print(f"  → 총 {len(df)}건 raw 수집 완료")

# ── 8) 필요한 컬럼만 빼고 이름 바꾸기 ──
cols = ["statsYm","ctpvNm","sggNm","totNmprCnt","hhCnt","hhNmpr"]
df = df[cols].copy()
df.columns = ["시점","시도","시군구","인구 수","세대 수","세대당 인구 수"]

# ── 9) 행정구역 계층별 필터링
# 예: "경기도 수원시 영통구" 입력 시,
#   시도 레벨 → 시도명("시도")만, 시군구는 빈칸
#   시군구 레벨 → 시도+시군구
#   읍면동 레벨 → 모두 채워짐
parts = args.region_name.split()
sido_name = parts[0]
sgg_name = parts[1] if len(parts)>=2 else None
emd_name = parts[2] if len(parts)==3 else None

def keep_row(r):
    if not sgg_name:
        # 시도만 요청: 시도 레벨(lv=1) 행만
        return pd.isna(r["시군구"]) or r["시군구"] == ""
    if not emd_name:
        # 시도+시군구 요청: 시도 레벨 OR (시군구 == sgg_name)
        return (pd.isna(r["시군구"]) or r["시군구"]=="") \
               or (r["시군구"] == sgg_name)
    # 시도+시군구+읍면동 요청: only 시군구 == full 입력 똑같이 나옴
    return r["시군구"] == emd_name

df = df[df.apply(keep_row, axis=1)].reset_index(drop=True)

# ── 10) 엑셀 저장 ──
with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    df.to_excel(writer, sheet_name='인구_세대', index=False)

print(f"✅ '{args.output}' 에 저장되었습니다.")
