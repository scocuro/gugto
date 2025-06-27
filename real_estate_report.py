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
    return sub.iloc[0]["법정동코드"][:5]

# ── 2) 커맨드라인 인자 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--lawd-cd',     help='5자리 시군구코드 직접 입력')
group.add_argument('--region-name', help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
parser.add_argument('--start-year',  type=int, default=2020, help='조회 시작 연도')
parser.add_argument('--output',      default='report.xlsx', help='출력 엑셀 파일명')
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

# ── 4) API 키 및 엔드포인트 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수를 설정하세요.")
    sys.exit(1)

BASE_SALE_URL  = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
BASE_RENT_URL  = "http://apis.data.go.kr/1613000/RTMSDataSvcRent/getRTMSDataSvcRent"
BASE_SILV_URL  = "http://apis.data.go.kr/1613000/RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade"

# ── 5) XML 호출 헬퍼 ──
def fetch_items(base_url, params):
    try:
        r = requests.get(base_url, params=params, timeout=30)
        r.raise_for_status()
    except Exception:
        # 500 등 서버 에러는 빈 목록으로 처리
        return []
    txt = r.text
    try:
        df = pd.read_xml(StringIO(txt), xpath='.//item', parser='etree')
    except Exception:
        return []
    return df.to_dict(orient='records')

# ── 6) 전체 수집 함수 ──
def collect_all(base_url, cols, date_key):
    today = datetime.today()
    start_year = args.start_year
    end_year, end_month = today.year, today.month
    rows = []
    for yyyy in range(start_year, end_year+1):
        m_max = end_month if yyyy==end_year else 12
        for mm in range(1, m_max+1):
            ymd = f"{yyyy}{mm:02d}"
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
                df = df.loc[:, [*cols, '거래일', '계약구분', '임대보증금', '월세금액']]\
                       .rename(columns={
                           '거래일':'dealDay',
                           '계약구분':'contractType',
                           '임대보증금':'deposit',
                           '월세금액':'monthlyRent'
                       })
                # 날짜 분리
                df['dealYear']  = int(yyyy)
                df['dealMonth'] = int(mm)
                # 숫자 변환
                if 'dealAmount' in df:
                    df['dealAmount'] = (
                        df['dealAmount'].astype(str)
                                      .str.replace(',','', regex=False)
                                      .astype(float)
                    )
                if 'deposit' in df:
                    df['deposit'] = (
                        df['deposit'].astype(str)
                                   .str.replace(',','', regex=False)
                                   .astype(float)
                    )
                if 'monthlyRent' in df:
                    df['monthlyRent'] = (
                        df['monthlyRent'].astype(str)
                                        .str.replace(',','', regex=False)
                                        .astype(float)
                    )
                if 'excluUseAr' in df:
                    df['excluUseAr_adj'] = (
                        df['excluUseAr'].astype(str)
                                        .str.replace(',','', regex=False)
                                        .astype(float)
                                        * 121/400
                    )
                rows.append(df)
                page += 1
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

# ── 7) 컬럼 정의 ──
sale_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr','거래금액','건축년도']
rent_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr']  # + deposit, monthlyRent, contractType
silv_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr']

# ── 8) 데이터 수집 ──
print("▶ 매매 수집…");   df_sale = collect_all(BASE_SALE_URL, sale_cols, 'DEAL_YMD')
print(f"  → {len(df_sale)}건 수집 완료")
print("▶ 전월세 수집…"); df_rent = collect_all(BASE_RENT_URL, rent_cols, 'DEAL_YMD')
print(f"  → {len(df_rent)}건 수집 완료")
print("▶ 분양권 수집…"); df_silv = collect_all(BASE_SILV_URL, silv_cols, 'DEAL_YMD')
print(f"  → {len(df_silv)}건 수집 완료")

# ── 9) 엑셀 작성 ──
with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    # Raw sheets
    df_sale.to_excel(writer, sheet_name='매매(raw)', index=False)
    df_rent.to_excel(writer, sheet_name='전세(raw)', index=False)
    df_silv.to_excel(writer, sheet_name='분양권(raw)', index=False)

    # 피벗 함수
    def make_pivot(df, valcol, name):
        if df.empty: return pd.DataFrame()
        g = df.groupby(['umdNm','aptNm','dealYear'], dropna=False)
        pv = g.agg(
            case_count = ('dealYear','size'),
            avg_value  = (valcol, 'mean'),
            avg_exclu  = ('excluUseAr_adj','mean')
        ).reset_index()
        # 열 순서 재배치
        years = sorted(pv['dealYear'].unique())
        cols = []
        for y in years:
            cols += [
                f"case_count_{y}",
                f"avg_value_{y}",
                f"avg_exclu_{y}",
            ]
        out = pd.DataFrame()
        out[['umdNm','aptNm']] = pv[['umdNm','aptNm']].drop_duplicates().reset_index(drop=True)
        for y in years:
            sub = pv[pv['dealYear']==y]
            out[f"case_count_{y}"] = out.merge(sub[['umdNm','aptNm','case_count']], on=['umdNm','aptNm'], how='left')['case_count']
            out[f"avg_value_{y}"]  = out.merge(sub[['umdNm','aptNm','avg_value' ]], on=['umdNm','aptNm'], how='left')['avg_value']
            out[f"avg_exclu_{y}"]  = out.merge(sub[['umdNm','aptNm','avg_exclu' ]], on=['umdNm','aptNm'], how='left')['avg_exclu']
        # 포맷 적용
        workbook  = writer.book
        fmt_num   = workbook.add_format({'num_format':'#,##0.00'})
        fmt_exclu = workbook.add_format({'num_format':'0.00'})
        for c in cols:
            fmt = fmt_exclu if c.startswith('avg_exclu') else fmt_num
            writer.sheets[name].set_column(f"{cols.index(c)+2}:{cols.index(c)+2}", None, fmt)
        return out[['umdNm','aptNm']+cols]

    # 피벗 시트
    make_pivot(df_sale, 'dealAmount', '매매(수정)').to_excel(writer, sheet_name='매매(수정)', index=False)
    make_pivot(df_rent, 'fixed_deposit', '전세(수정)').to_excel(writer, sheet_name='전세(수정)', index=False)
    make_pivot(df_silv, 'dealAmount',    '분양권(수정)').to_excel(writer, sheet_name='분양권(수정)', index=False)

print(f"✅ '{args.output}' 에 저장되었습니다.")
