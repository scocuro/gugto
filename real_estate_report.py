#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
공공데이터 실거래 리포트 생성기
- 시군구 코드 CSV 로드
- 매매·전월세·분양권 원본 시트
- 전월세 fixed_deposit 계산
- 매매(수정), 전세(수정), 분양권(수정) 피벗 시트
- 이번달까지만 데이터 수집
"""

import argparse
import os
import sys
import requests
import pandas as pd
import io
from datetime import datetime

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
        city, gu = parts[1], parts[2]
        full_sigungu = city + gu
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == full_sigungu)
        ]
    else:
        raise ValueError("‘시도 종군구’ 또는 ‘시도 시군구 읍면동’ 형식으로 입력해 주세요.")
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")
    full_code = sub.iloc[0]["법정동코드"]
    return full_code[:5]

# ── 2) 커맨드라인 인자 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--lawd-cd',     help='직접 사용할 법정동코드(앞 5자리)')
group.add_argument('--region-name', help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
parser.add_argument('--start-year',  type=int, default=2020, help='조회 시작 연도(YYYY)')
parser.add_argument('--output',      default='report.xlsx', help='출력 엑셀 파일명')
args = parser.parse_args()

# ── 3) region_code 결정 ──
if args.lawd_cd:
    region_code = args.lawd_cd
else:
    try:
        region_code = get_region_code(args.region_name)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)

start_year  = args.start_year
output_file = args.output

# ── 4) 환경변수에서 API 키 로드 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: 환경변수 PUBLIC_DATA_API_KEY에 API 키를 설정하세요.")
    sys.exit(1)

# ── 5) API 엔드포인트 설정 ──
BASE_SALE_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
BASE_RENT_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcRent/getRTMSDataSvcRent"
BASE_SILV_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade"
page_size = 1000
rent_conversion_rate = 0.06  # 6%

def fetch_data(url: str, params: dict) -> pd.DataFrame:
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    txt = resp.text
    # StringIO로 감싸서 FutureWarning 회피
    df = pd.read_xml(io.StringIO(txt), xpath=".//item", parser="lxml")
    return df

def collect_all(url: str, cols: list, date_field: str) -> pd.DataFrame:
    records = []
    now = datetime.now()
    for year in range(start_year, now.year + 1):
        months = range(1, now.month + 1) if year == now.year else range(1, 13)
        for month in months:
            deal_ym = f"{year}{month:02d}"
            page = 1
            while True:
                params = {
                    'serviceKey': API_KEY,
                    'LAWD_CD':    region_code,
                    'DEAL_YMD':   deal_ym,
                    'pageNo':     page,
                    'numOfRows':  page_size,
                    'resultType': 'xml'
                }
                try:
                    df = fetch_data(url, params)
                except Exception as e:
                    print(f"Warning: {deal_ym} p{page} 요청 실패: {e}")
                    break
                if df.empty:
                    break
                df = df.loc[:, cols]
                df['dealYear']  = int(deal_ym[:4])
                df['dealMonth'] = int(deal_ym[4:6])
                df['dealDay']   = df['dealDay'].astype(int)
                records.append(df)
                if len(df) < page_size:
                    break
                page += 1
    if records:
        return pd.concat(records, ignore_index=True)
    else:
        return pd.DataFrame(columns=cols + ['dealYear','dealMonth','dealDay'])

# ── 6) 컬럼 정의 및 데이터 수집 ──
sale_cols = ['sggCD','umdNm','aptNm','jibun','excluUseAr','dealYear','dealMonth','dealDay','dealAmount','floor','buildYear']
rent_cols = ['sggCD','umdNm','aptNm','jibun','excluUseAr','dealYear','dealMonth','dealDay','deposit','monthlyRent','floor','buildYear','contractType','useRRRight']
silv_cols = ['sggCD','umdNm','aptNm','jibun','excluUseAr','dealYear','dealMonth','dealDay','dealAmount','ownershipGbn']

print("▶ 매매 수집…")
df_sale = collect_all(BASE_SALE_URL, sale_cols, 'DEAL_YMD')
print(f"  → {len(df_sale)}건 수집 완료")

print("▶ 전월세 수집…")
df_rent = collect_all(BASE_RENT_URL, rent_cols, 'DEAL_YMD')
print(f"  → {len(df_rent)}건 수집 완료")

print("▶ 분양권 수집…")
df_silv = collect_all(BASE_SILV_URL, silv_cols, 'DEAL_YMD')
print(f"  → {len(df_silv)}건 수집 완료")

# ── 7) 전월세 fixed_deposit 계산 ──
df_rent['fixed_deposit'] = (
    df_rent['monthlyRent'].str.replace(',','').astype(float) * 12 / rent_conversion_rate
    + df_rent['deposit'].str.replace(',','').astype(float)
)

# ── 8) 피벗 함수 ──
def make_pivot(df: pd.DataFrame, value_col: str, sheet_name: str) -> pd.DataFrame:
    df = df.copy()
    df['excluUseAr_adj'] = df['excluUseAr'].str.replace(',','').astype(float) * 121 / 400
    # 그룹핑
    grouped = df.groupby(['umdNm','aptNm','dealYear'], dropna=False)
    agg = (
        grouped
        .agg(
            case_count = ('dealYear', 'size'),
            avg_value  = (value_col, 'mean'),
            avg_exclu  = ('excluUseAr_adj', 'mean')
        )
        .reset_index()
    )
    # 피벗
    pv = agg.pivot(index=['umdNm','aptNm'], columns='dealYear')
    # 컬럼 재정렬: 년도 오름차순, 각 년도에 case,avg_value,avg_exclu 순
    years = sorted(agg['dealYear'].unique())
    new_cols = []
    for y in years:
        new_cols += [
            ('case_count', y),
            ('avg_value',  y),
            ('avg_exclu',  y)
        ]
    pv = pv.reorder_levels([0,1], axis=1)[new_cols]
    # 포맷팅
    # exclu, avg_exclu → 소수점 2자리 반올림
    # 수치(value_col, deposit 등) → 천단위 콤마+소수점2자리
    def fmt_num(x):
        return f"{x:,.2f}" if pd.notna(x) else ""
    def fmt_per(x):
        return f"{round(x,2):.2f}" if pd.notna(x) else ""

    for col in pv.columns:
        if col[0] == 'avg_exclu':
            pv[col] = pv[col].map(fmt_per)
        else:
            pv[col] = pv[col].map(fmt_num)

    pv.columns = [f"{c[0]}_{c[1]}" for c in pv.columns]
    pv = pv.reset_index()
    pv.name = sheet_name
    return pv

# ── 9) 엑셀로 저장 ──
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    df_sale.to_excel(writer, sheet_name='매매(raw)', index=False)
    df_rent.to_excel(writer, sheet_name='전월세(raw)', index=False)
    df_silv.to_excel(writer, sheet_name='분양권(raw)', index=False)

    sale_pv = make_pivot(df_sale, 'dealAmount',   '매매(수정)')
    rent_pv = make_pivot(df_rent, 'fixed_deposit','전세(수정)')
    silv_pv = make_pivot(df_silv, 'dealAmount',   '분양권(수정)')

    sale_pv.to_excel(writer, sheet_name='매매(수정)',   index=False)
    rent_pv.to_excel(writer, sheet_name='전세(수정)',   index=False)
    silv_pv.to_excel(writer, sheet_name='분양권(수정)', index=False)

print(f"✅ 리포트가 '{output_file}' 로 저장되었습니다.")
