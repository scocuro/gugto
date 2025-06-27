# real_estate_report.py

import argparse
import os
import sys
import requests
import pandas as pd
from io import StringIO
from datetime import datetime

# ── 컬럼 정의 ──
sale_cols = [
    'sggCd', 'umdNm', 'aptNm', 'jibun', 'excluUseAr',
    'dealYear', 'dealMonth', 'dealDay',
    'dealAmount', 'floor', 'buildYear'
]
rent_cols = [
    'sggCd', 'umdNm', 'aptNm', 'jibun', 'excluUseAr',
    'dealYear', 'dealMonth', 'dealDay',
    'deposit', 'monthlyRent', 'floor', 'buildYear'
]
silv_cols = [
    'sggCd', 'umdNm', 'aptNm', 'jibun', 'excluUseAr',
    'dealYear', 'dealMonth', 'dealDay',
    'dealAmount', 'ownershipGbn', 'floor', 'buildYear'
]

# ── 1) 시군구 코드 CSV 로드 ──
CSV_PATH = "code_raw.csv"
try:
    csv_df = pd.read_csv(CSV_PATH, dtype=str, encoding='utf-8')
except Exception as e:
    print(f"ERROR: 시군구 코드 CSV를 불러오는 데 실패했습니다 ({CSV_PATH}): {e}")
    sys.exit(1)

def get_region_code(name: str) -> str:
    parts = name.split()
    sido = parts[0]
    if len(parts) == 2:
        sigungu = parts[1]
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == sigungu) &
            (csv_df["읍면동명"].isna())
        ]
    elif len(parts) == 3:
        city, gu = parts[1], parts[2]
        full = city + gu
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == full)
        ]
    else:
        raise ValueError("‘시도 시군구’ 또는 ‘시도 시군구 읍면동’ 형식으로 입력하세요.")
    if sub.empty:
        raise LookupError(f"'{name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")
    return sub.iloc[0]["법정동코드"][:5]

# ── 2) CLI 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--lawd-cd',     help='법정동코드 5자리')
group.add_argument('--region-name', help='예: 충청남도 천안시 동남구')
parser.add_argument('--start-year', type=int, default=2020, help='시작 연도')
parser.add_argument('--output',     default='report.xlsx', help='출력 파일명')
args = parser.parse_args()

if args.lawd_cd:
    region_code = args.lawd_cd
else:
    try:
        region_code = get_region_code(args.region_name)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)

API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)

BASE_SALE_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
BASE_RENT_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcRent/getRTMSDataSvcRent"
BASE_SILV_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade"

# ── 3) 데이터 수집 함수 ──
def collect_all(base_url, cols, date_key="DEAL_YMD"):
    recs = []
    page_size = 1000
    this_year = datetime.now().year
    for yr in range(args.start_year, this_year + 1):
        for m in range(1, 13):
            ymd = f"{yr}{m:02d}"
            p = 1
            while True:
                params = {
                    'serviceKey': API_KEY,
                    'LAWD_CD':    region_code,
                    date_key:     ymd,
                    'pageNo':     p,
                    'numOfRows':  page_size,
                    'resultType': 'xml'
                }
                try:
                    r = requests.get(base_url, params=params, timeout=30)
                    r.raise_for_status()
                    df = pd.read_xml(StringIO(r.text), xpath='//item', parser='lxml')
                except Exception as e:
                    print(f"Warning: {ymd} p{p} 요청 실패: {e}")
                    break
                if df.empty:
                    break
                df = df[[c for c in cols if c in df.columns]]
                recs.append(df)
                if len(df) < page_size:
                    break
                p += 1
    return pd.concat(recs, ignore_index=True) if recs else pd.DataFrame(columns=cols)

# ── 4) 수집 ──
print("▶ 매매 수집…")
df_sale = collect_all(BASE_SALE_URL, sale_cols)
print("▶ 전월세 수집…")
df_rent = collect_all(BASE_RENT_URL, rent_cols)
print("▶ 분양권 수집…")
df_silv = collect_all(BASE_SILV_URL, silv_cols)

# ── 5) 숫자형 정리 ──
if not df_sale.empty:
    df_sale['dealAmount'] = (
        df_sale['dealAmount']
        .astype(str).str.replace(',', '')
        .astype(float)
    )

if not df_silv.empty:
    df_silv['dealAmount'] = (
        df_silv['dealAmount']
        .astype(str).str.replace(',', '')
        .astype(float)
    )

if not df_rent.empty:
    df_rent['monthlyRent'] = (
        df_rent['monthlyRent']
        .astype(str).str.replace(',', '')
        .astype(float)
    )
    df_rent['deposit'] = (
        df_rent['deposit']
        .astype(str).str.replace(',', '')
        .astype(float)
    )
    rent_conv = 0.06
    df_rent['fixed_deposit'] = (
        df_rent['monthlyRent'] * 12 / rent_conv
        + df_rent['deposit']
    )

# ── 6) 피벗 함수 ──
def make_pivot(df, val_col):
    if df.empty:
        return pd.DataFrame()
    df['excluUseAr'] = (
        df['excluUseAr']
        .astype(str).str.replace(',', '').astype(float)
    )
    df['exclu_adj'] = df['excluUseAr'] * 121 / 400
    df['dealYear']  = df['dealYear'].astype(int)

    grp = df.groupby(['umdNm','aptNm','dealYear'], dropna=False)
    agg = grp.agg(
        case_count=('dealYear','size'),
        avg_val    =(val_col,'mean'),
        avg_exclu  =('exclu_adj','mean')
    ).reset_index()

    yrs = sorted(agg['dealYear'].unique())
    cols = []
    for y in yrs:
        cols += [
            f"case_count_{y}",
            f"avg_value_{y}",
            f"avg_exclu_{y}"
        ]

    pv = agg.pivot_table(
        index=['umdNm','aptNm'], columns='dealYear',
        values=['case_count','avg_val','avg_exclu']
    )
    pv.columns = cols
    pv = pv.reset_index()

    # 포맷
    for c in pv.columns:
        if c.startswith('case_count_'):
            pv[c] = pv[c].fillna(0).astype(int)
        elif c.startswith('avg_exclu_'):
            pv[c] = pv[c].round(2)
        else:
            pv[c] = pv[c].map(lambda x: f"{x:,.2f}" if pd.notna(x) else "")

    return pv

# ── 7) 엑셀 저장 ──
with pd.ExcelWriter(args.output, engine='xlsxwriter') as w:
    df_sale.to_excel(w, sheet_name='매매(Raw)', index=False)
    df_rent.to_excel(w, sheet_name='전월세(Raw)', index=False)
    df_silv.to_excel(w, sheet_name='분양권(Raw)', index=False)

    pv_sale = make_pivot(df_sale,   'dealAmount')
    pv_rent = make_pivot(df_rent,   'fixed_deposit')
    pv_silv = make_pivot(df_silv,   'dealAmount')

    if not pv_sale.empty: pv_sale.to_excel(w, sheet_name='매매(수정)', index=False)
    if not pv_rent.empty: pv_rent.to_excel(w, sheet_name='전세(수정)', index=False)
    if not pv_silv.empty: pv_silv.to_excel(w, sheet_name='분양권(수정)', index=False)

print(f"리포트가 '{args.output}'에 저장되었습니다.")
