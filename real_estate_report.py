#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
real_estate_report.py

• Sales  : getRTMSDataSvcAptTrade
• Rent    : getRTMSDataSvcAptRent
• 시군구코드(csv) → region_code
• --lawd-cd / --region-name, --start-year, --built-after, --output
• TLS1.2 고정
• XML 네임스페이스 제거 후 파싱
• 페이징 수집
• 건축년도 필터
• 필요한 컬럼만 선택
• ExcelWriter: sheet "Sales" / sheet "Rent"
"""

import os, sys, ssl, re, argparse
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

# ─────────────────────────────────────────────────────────────────────────────
# 1) 시군구 코드 CSV 로드
CSV_PATH = "code_raw.csv"
try:
    csv_df = pd.read_csv(CSV_PATH, dtype=str, encoding="utf-8")
except Exception as e:
    print(f"ERROR: 시군구 코드 CSV 로드 실패 ({CSV_PATH}): {e}")
    sys.exit(1)

def get_region_code(region_name: str) -> str:
    parts = region_name.strip().split()
    if len(parts)==2:
        sido, gugun = parts
        sub = csv_df[(csv_df["시도명"]==sido)&(csv_df["시군구명"]==gugun)&(csv_df["읍면동명"].isna())]
    elif len(parts)==3:
        sido, city, gu = parts
        full = city+gu
        sub = csv_df[(csv_df["시도명"]==sido)&(csv_df["시군구명"]==full)]
    else:
        raise ValueError("‘시도 종군구’ 또는 ‘시도 시군구 읍면동’ 형식으로 입력하세요.")
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 찾을 수 없습니다.")
    return sub.iloc[0]["법정동코드"][:5]

# ─────────────────────────────────────────────────────────────────────────────
# 2) 인자 파싱
p = argparse.ArgumentParser(description="공공데이터 실거래 & 전월세 Raw Data 수집기")
grp = p.add_mutually_exclusive_group(required=True)
grp.add_argument('--lawd-cd',     help='5자리 법정동(시군구) 코드 직접 입력')
grp.add_argument('--region-name', help='시도+시군구 명칭 (예: 충남 천안시 동남구)')
p.add_argument('--start-year',  type=int, default=2025, help='조회 시작 연도')
p.add_argument('--built-after', type=int, default=2015, help='준공 연도 이후 필터')
p.add_argument('--output',      default='real_estate_report.xlsx', help='출력 엑셀 파일명')
args = p.parse_args()

if args.lawd_cd:
    region_code = args.lawd_cd
else:
    try:
        region_code = get_region_code(args.region_name)
    except Exception as e:
        print("ERROR:", e); sys.exit(1)

start_year  = args.start_year
built_after = args.built_after
out_file    = args.output

# ─────────────────────────────────────────────────────────────────────────────
# 3) API_KEY + TLS1.2 세션
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수 설정 필요"); sys.exit(1)

class TLS12Adapter(HTTPAdapter):
    def init_poolmanager(self,*a,**k):
        ctx = create_urllib3_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        k['ssl_context']=ctx
        return super().init_poolmanager(*a,**k)

sess = requests.Session()
sess.mount("https://", TLS12Adapter())

# ─────────────────────────────────────────────────────────────────────────────
# 4) 엔드포인트 & 공통
BASE_SALES_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
BASE_RENT_URL  = "http://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
PAGE_SIZE = 1000

# 공통컬럼 + 렌트전용
COMMON = ['sggCd','umdNm','aptNm','jibun','excluUseAr','dealYear','dealMonth','dealDay','floor','buildYear']
RENT_ONLY = ['deposit','monthlyRent','contractType','useRRRight']

def fetch_df(url, ym, page):
    params = {
        'serviceKey': API_KEY,
        'LAWD_CD':    region_code,
        'DEAL_YMD':   ym,
        'pageNo':     page,
        'numOfRows':  PAGE_SIZE,
    }
    r = sess.get(url, params=params, timeout=30)
    r.raise_for_status()
    txt = r.content.decode('utf-8')
    # 네임스페이스 제거
    txt = re.sub(r'\sxmlns="[^"]+"','', txt, count=1)
    return pd.read_xml(txt, xpath='.//item', parser='lxml')

def collect_all(url, want_cols):
    recs = []
    now = datetime.now()
    for yy in range(start_year, now.year+1):
        last_m = now.month if yy==now.year else 12
        for mm in range(1, last_m+1):
            ym = f"{yy}{mm:02d}"
            p=1; month_buf=[]
            while True:
                try:
                    df = fetch_df(url, ym, p)
                except Exception as e:
                    print(f"WARN: {ym} p{p} 요청/파싱 실패: {e}")
                    break
                if df.empty: break
                # 타입 변환 / 필터
                df['buildYear'] = df['buildYear'].astype(int)
                df = df[df['buildYear']>=built_after]
                # dealAmount→float, excluUseAr→float
                if 'dealAmount' in df: df['dealAmount']=df['dealAmount'].str.replace(',','').astype(float)
                df['excluUseAr']=df['excluUseAr'].astype(float)
                # 전월세 전용 numeric
                if 'deposit' in df:    df['deposit']=df['deposit'].str.replace(',','').astype(float)
                if 'monthlyRent' in df:df['monthlyRent']=df['monthlyRent'].str.replace(',','').astype(float)
                # 칼럼 추출
                subset = df[[c for c in want_cols if c in df.columns]]
                month_buf.append(subset)
                if len(df)<PAGE_SIZE: break
                p+=1
            if month_buf:
                recs.extend(month_buf)
            print(f"[INFO] {ym} → {sum(len(x) for x in month_buf)}건")
    return pd.concat(recs,ignore_index=True) if recs else pd.DataFrame()

# ─────────────────────────────────────────────────────────────────────────────
print("▶ 매매(Sales) 수집 중…")
sales_cols = COMMON + []  # 매매는 deposit 등 없음
sales_df = collect_all(BASE_SALES_URL, sales_cols)

print("\n▶ 전월세(Rent) 수집 중…")
rent_cols  = COMMON + RENT_ONLY
rent_df    = collect_all(BASE_RENT_URL, rent_cols)

# ─────────────────────────────────────────────────────────────────────────────
if sales_df.empty and rent_df.empty:
    print("❌ 아무 데이터도 수집되지 않았습니다."); sys.exit(0)

# ─────────────────────────────────────────────────────────────────────────────
# 5) 엑셀 저장
with pd.ExcelWriter(out_file, engine='xlsxwriter') as wr:
    if not sales_df.empty:
        sales_df.to_excel(wr, sheet_name='Sales', index=False)
    if not rent_df.empty:
        rent_df.to_excel(wr, sheet_name='Rent',  index=False)

print(f"\n✅ 저장 완료 → {out_file}")
