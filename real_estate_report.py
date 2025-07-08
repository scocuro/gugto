#!/usr/bin/env python3
# real_estate_report.py

import argparse
import os
import sys
import requests
import pandas as pd
from datetime import datetime
from io import StringIO

# ── 1) 시군구 코드 CSV 로드 ──
CSV_PATH = "code_raw.csv"
try:
    csv_df = pd.read_csv(CSV_PATH, encoding="utf-8", dtype=str)
except Exception as e:
    print(f"ERROR: 시군구 코드 CSV를 불러오는 데 실패했습니다 ({CSV_PATH}): {e}")
    sys.exit(1)

def get_region_code(region_name: str) -> str:
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
    return sub.iloc[0]["법정동코드"][:5]

# ── 2) CLI 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
grp = parser.add_mutually_exclusive_group(required=True)
grp.add_argument('--lawd-cd',     help='5자리 시군구코드를 직접 입력')
grp.add_argument('--region-name', help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
parser.add_argument('--start-year', type=int, default=2020, help='조회 시작 연도')
parser.add_argument('--output',     default='report.xlsx', help='출력 엑셀 파일명')
args = parser.parse_args()

# ── 3) 시군구코드 결정 ──
if args.lawd_cd:
    region_code = args.lawd_cd
else:
    try:
        region_code = get_region_code(args.region_name)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)

# ── 4) API 키 & 엔드포인트 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)

BASE_SALE_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
BASE_RENT_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
BASE_SILV_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade"

# ── 5) API 호출 + XML→DataFrame 헬퍼 ──
def fetch_items(url, params):
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
    except Exception:
        return []
    txt = r.text
    try:
        df = pd.read_xml(StringIO(txt), xpath='.//item', parser='etree')
    except Exception:
        return []
    return df.to_dict(orient='records')

# ── 6) 전체 데이터 수집 ──
def collect_all(base_url, cols, date_key):
    today = datetime.today()
    rows = []
    for yy in range(args.start_year, today.year+1):
        max_m = today.month if yy == today.year else 12
        for mm in range(1, max_m+1):
            ymd = f"{yy}{mm:02d}"
            page = 1
            while True:
                params = {
                    'serviceKey': API_KEY,
                    'LAWD_CD':    region_code,
                    date_key:     ymd,
                    'pageNo':     page,
                    'numOfRows':  1000,
                    'resultType': 'xml'
                }
                recs = fetch_items(base_url, params)
                if not recs:
                    break
                df = pd.DataFrame(recs)
                df = df.loc[:, [*cols, 'dealYear','dealMonth','dealDay']]
                # 숫자형 변환
                if 'dealAmount' in df:
                    df['dealAmount'] = (
                        df['dealAmount']
                        .astype(str).str.replace(',','',regex=False)
                        .astype(float)
                    )
                if 'deposit' in df:
                    df['deposit'] = (
                        df['deposit']
                        .astype(str).str.replace(',','',regex=False)
                        .astype(float)
                    )
                if 'monthlyRent' in df:
                    df['monthlyRent'] = (
                        df['monthlyRent']
                        .astype(str).str.replace(',','',regex=False)
                        .astype(float)
                    )
                if 'excluUseAr' in df:
                    df['excluUseAr_adj'] = (
                        df['excluUseAr'].astype(str)
                                       .str.replace(',','',regex=False)
                                       .astype(float)
                        * 121/400
                    )
                rows.append(df)
                page += 1
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

# ── 7) 컬럼 리스트 정의 ──
sale_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','dealAmount','buildYear']
rent_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','deposit','monthlyRent','contractType']
silv_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','dealAmount']

# ── 8) 실제 수집 ──
print("▶ 매매 수집…");   df_sale = collect_all(BASE_SALE_URL, sale_cols, 'DEAL_YMD')
print(f"  → {len(df_sale)}건 수집 완료")
print("▶ 전월세 수집…"); df_rent = collect_all(BASE_RENT_URL, rent_cols, 'DEAL_YMD')
print(f"  → {len(df_rent)}건 수집 완료")
print("▶ 분양권 수집…"); df_silv = collect_all(BASE_SILV_URL, silv_cols, 'DEAL_YMD')
print(f"  → {len(df_silv)}건 수집 완료")

# ── 9) 칼럼명 한글 매핑 ┙
COL_MAP = {
    'sggCd': '시군구코드',
    'sggNm': '시군구',
    'umdNm': '동',
    'aptNm': '건물명',
    'jibun': '지번',
    'excluUseAr': '전용면적',
    'dealAmount': '거래가액',
    'dealYear': '거래년도',
    'dealMonth': '거래월',
    'dealDay': '거래일',
    'excluUseAr_adj': '전용면적(평)',
    'deposit': '보증금',
    'monthlyRent': '월세'
}

# ── 10) 엑셀 작성 ──
with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    # raw 시트
    for df, name in [(df_sale, '매매(raw)'), (df_rent, '전세(raw)'), (df_silv, '분양권(raw)')]:
        if not df.empty:
            df = df.rename(columns=COL_MAP)
        df.to_excel(writer, sheet_name=name, index=False)

    # 피벗 및 매핑 함수
    def make_pivot(df, valcol):
        if df.empty: return pd.DataFrame()
        df = df.rename(columns=COL_MAP)
        g  = df.groupby(['동','건물명','거래년도'], dropna=False)
        pv = (g.agg(
            **{f'거래건수({y})': ('거래년도','size') for y in sorted(df['거래년도'].unique())},
            **{f'평균 거래가액({y})': (f'거래가액', 'mean') for y in sorted(df['거래년도'].unique())},
            **{f'평균 전용면적({y})': ('전용면적(평)', 'mean') for y in sorted(df['거래년도'].unique())}
        )).reset_index()
        return pv

    # 수정된 피벗 시트
    make_pivot(df_sale, '거래가액')  .to_excel(writer, sheet_name='매매(수정)', index=False)
    make_pivot(df_rent, '보증금')     .to_excel(writer, sheet_name='전세(수정)', index=False)
    make_pivot(df_silv, '거래가액').to_excel(writer, sheet_name='분양권(수정)', index=False)

print(f"✅ '{args.output}' 에 저장되었습니다.")
