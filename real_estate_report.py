# real_estate_report.py

import argparse
import os
import sys
import requests
import pandas as pd
import ssl
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
        full_sigungu = parts[1] + parts[2]
        sub = csv_df[
            (csv_df["시도명"]    == sido) &
            (csv_df["시군구명"] == full_sigungu)
        ]
    else:
        raise ValueError("‘시도 종군구’ 또는 ‘시도 시군구 읍면동’ 형식으로 입력해 주세요.")
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")
    return sub.iloc[0]["법정동코드"][:5]

# ── 2) 인자 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--lawd-cd',     help='법정동코드(앞5자리)')
group.add_argument('--region-name', help='시도+시군구 명칭')
parser.add_argument('--start-year',  type=int, default=2020)
parser.add_argument('--output',      default='report.xlsx')
args = parser.parse_args()

if args.lawd_cd:
    region_code = args.lawd_cd
else:
    try:
        region_code = get_region_code(args.region_name)
    except Exception as e:
        print("ERROR:", e); sys.exit(1)

start_year = args.start_year
output_file = args.output

API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: PUBLIC_DATA_API_KEY 환경변수에 API 키를 설정하세요."); sys.exit(1)
print(f"[DEBUG] using region_code = {region_code}")
print(f"[DEBUG] API_KEY length = {len(API_KEY)}")

# ── 엔드포인트 ──
BASE_SALE_URL   = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
BASE_RENT_URL   = "http://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
BASE_SILV_URL   = "http://apis.data.go.kr/1613000/RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade"

# ── 전월세 환산율 ──
RENT_CONV_RATE = 0.06  # 6%

# ── 컬럼 정의 ──
sale_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr',
             'dealYear','dealMonth','dealDay','dealAmount','floor','buildYear']
rent_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr',
             'dealYear','dealMonth','dealDay','deposit','monthlyRent','floor','buildYear','contractType','useRRRight']
silv_cols = ['sggCd','umdNm','aptNm','jibun','excluUseAr',
             'dealYear','dealMonth','dealDay','ownershipGbn','floor','buildYear']

def collect_all(base_url, cols, ym_param='DEAL_YMD'):
    records = []
    page_size = 1000
    current_year = datetime.now().year
    for year in range(start_year, current_year+1):
        for month in range(1,13):
            ymd = f"{year}{month:02d}"
            page=1
            while True:
                params = {
                    'serviceKey': API_KEY,
                    'LAWD_CD':    region_code,
                    ym_param:     ymd,
                    'pageNo':     page,
                    'numOfRows':  page_size,
                    'resultType': 'xml'
                }
                try:
                    resp = requests.get(base_url, params=params, timeout=30)
                    resp.raise_for_status()
                    txt = resp.text
                    df = pd.read_xml(txt, xpath='.//item', parser='lxml')
                except Exception as e:
                    # 더이상 데이터 없거나 오류 시 중단
                    break
                if df.empty: break
                # 필요한 컬럼만
                df = df.loc[:, df.columns.intersection(cols)]
                # 전월세 고정보증금 계산
                if 'monthlyRent' in df.columns:
                    df['monthlyRent'] = df['monthlyRent'].astype(str).str.replace(',','').astype(float)
                    df['deposit']     = df['deposit'].astype(str).str.replace(',','').astype(float)
                    df['fixed_deposit'] = (df['monthlyRent']*12/RENT_CONV_RATE + df['deposit'])
                # excluUseAr 숫자로
                df['excluUseAr'] = df['excluUseAr'].astype(str).str.replace(',','').astype(float)
                # dealAmount도 숫자로
                if 'dealAmount' in df.columns:
                    df['dealAmount'] = df['dealAmount'].astype(str).str.replace(',','').astype(float)
                records.append(df)
                if len(df) < page_size: break
                page += 1
    return pd.concat(records, ignore_index=True) if records else pd.DataFrame()

print("▶ 매매(Sales) 수집 중…")
df_sale = collect_all(BASE_SALE_URL, sale_cols, 'DEAL_YMD')
print(f"  → {len(df_sale)}건 수집 완료")

print("▶ 전월세(Rent) 수집 중…")
df_rent = collect_all(BASE_RENT_URL, rent_cols, 'DEAL_YMD')
print(f"  → {len(df_rent)}건 수집 완료")

print("▶ 분양권(Silver) 수집 중…")
df_silv = collect_all(BASE_SILV_URL, silv_cols, 'DEAL_YMD')
print(f"  → {len(df_silv)}건 수집 완료")

# ── 피벗 함수 ──
def make_pivot(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    # excluUseAr 비율 조정 (예: 121/400 비례)
    df['excluUseAr_adj'] = df['excluUseAr'] * 121 / 400

    pivot = (
        df.groupby(["umdNm","aptNm","dealYear"])
          .agg(
              case_count=("dealYear","size"),
              avg_value =(value_col,"mean"),
              avg_exclu =("excluUseAr_adj","mean")
          )
          .unstack("dealYear")
    )
    years   = sorted(pivot.columns.get_level_values(1).unique())
    metrics = ["case_count","avg_value","avg_exclu"]
    new_cols = []
    for y in years:
        for m in metrics:
            new_cols.append(f"{m}_{y}")

    result = pivot.reset_index()
    return result[["umdNm","aptNm"] + new_cols]

# ── Excel 출력 ──
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    # 원본 시트
    df_sale.to_excel(writer, sheet_name='매매(원본)', index=False)
    df_rent.to_excel(writer, sheet_name='전월세(원본)', index=False)
    df_silv.to_excel(writer, sheet_name='분양권(원본)', index=False)

    # 수정 시트
    sale_pv = make_pivot(df_sale, 'dealAmount')
    rent_pv = make_pivot(df_rent, 'fixed_deposit')
    silv_pv = make_pivot(df_silv, 'dealAmount')

    sale_pv.to_excel(writer, sheet_name='매매(수정)', index=False)
    rent_pv.to_excel(writer, sheet_name='전세(수정)', index=False)
    silv_pv.to_excel(writer, sheet_name='분양권(수정)', index=False)

    # 포맷 정의
    wb  = writer.book
    fmt_amt   = wb.add_format({'num_format':'#,##0.00'})
    fmt_pct   = wb.add_format({'num_format':'0.00'})

    # 모든 수정 시트에 포맷 적용
    for name, df in [('매매(수정)', sale_pv),
                     ('전세(수정)', rent_pv),
                     ('분양권(수정)', silv_pv)]:
        ws = writer.sheets[name]
        # 각 컬럼별 포맷
        for idx, col in enumerate(df.columns):
            # A,B 컬럼은 텍스트
            if idx < 2: continue
            if col.startswith('avg_exclu'):
                ws.set_column(idx, idx, 12, fmt_pct)
            else:
                ws.set_column(idx, idx, 12, fmt_amt)

print(f"리포트가 '{output_file}' 로 저장되었습니다.")
