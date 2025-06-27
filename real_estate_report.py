# real_estate_report.py

import argparse
import os
import sys
import requests
import pandas as pd
from io import StringIO
from datetime import datetime

# ── 0) 수집할 컬럼 리스트 정의 ──
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
CSV_PATH = "code_raw.csv"  # 프로젝트 루트에 두세요
try:
    csv_df = pd.read_csv(CSV_PATH, dtype=str, encoding='utf-8')
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
        raise ValueError("‘시도 시군구’ 또는 ‘시도 시군구 읍면동’ 형식으로 입력해 주세요.")
    if sub.empty:
        raise LookupError(f"'{region_name}'에 맞는 코드를 CSV에서 찾을 수 없습니다.")
    return sub.iloc[0]["법정동코드"][:5]

# ── 2) 커맨드라인 인자 파싱 ──
parser = argparse.ArgumentParser(description="공공데이터 실거래 리포트 생성기")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--lawd-cd',     help='직접 사용할 법정동코드(5자리)')
group.add_argument('--region-name', help='시도+시군구 명칭 (예: 충청남도 천안시 동남구)')
parser.add_argument('--start-year',  type=int, default=2020, help='조회 시작 연도')
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

# ── 4) API 키 ──
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    print("ERROR: 환경변수 PUBLIC_DATA_API_KEY에 API 키를 설정하세요.")
    sys.exit(1)

BASE_SALE_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
BASE_RENT_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcRent/getRTMSDataSvcRent"
BASE_SILV_URL = "http://apis.data.go.kr/1613000/RTMSDataSvcSilvTrade/getRTMSDataSvcSilvTrade"

def collect_all(base_url: str, cols: list, date_param: str = "DEAL_YMD") -> pd.DataFrame:
    records = []
    page_size = 1000
    now = datetime.now()
    for year in range(args.start_year, now.year + 1):
        for month in range(1, 13):
            ymd = f"{year}{month:02d}"
            page = 1
            while True:
                params = {
                    'serviceKey': API_KEY,
                    'LAWD_CD':    region_code,
                    date_param:   ymd,
                    'pageNo':     page,
                    'numOfRows':  page_size,
                    'resultType': 'xml'
                }
                try:
                    r = requests.get(base_url, params=params, timeout=30)
                    r.raise_for_status()
                    xml = r.text
                    # StringIO로 감싸야 FutureWarning 방지
                    df = pd.read_xml(StringIO(xml), xpath='//item', parser='lxml')
                except Exception as e:
                    print(f"Warning: {ymd} p{page} 요청 실패: {e}")
                    break
                if df.empty:
                    break
                # select only our cols (some APIs return extras)
                df = df.loc[:, [c for c in cols if c in df.columns]]
                records.append(df)
                if len(df) < page_size:
                    break
                page += 1
    if not records:
        return pd.DataFrame(columns=cols)
    return pd.concat(records, ignore_index=True)

# ── 5) 데이터 수집 ──
print("▶ 매매(Sales) 수집 중…")
df_sale = collect_all(BASE_SALE_URL, sale_cols)

print("▶ 전월세(Rent) 수집 중…")
df_rent = collect_all(BASE_RENT_URL, rent_cols)

print("▶ 분양권(Silver) 수집 중…")
df_silv = collect_all(BASE_SILV_URL, silv_cols)

# ── 6) 전월세 fixed_deposit 추가 ──
rent_conversion_rate = 0.06
if not df_rent.empty:
    df_rent['monthlyRent'] = (
        df_rent['monthlyRent']
        .astype(str)
        .str.replace(',', '')
        .astype(float)
    )
    df_rent['deposit'] = (
        df_rent['deposit']
        .astype(str)
        .str.replace(',', '')
        .astype(float)
    )
    df_rent['fixed_deposit'] = (
        df_rent['monthlyRent'] * 12 / rent_conversion_rate
        + df_rent['deposit']
    )

# ── 7) Pivot 함수 ──
def make_pivot(df, value_col, sheet_name):
    if df.empty:
        return pd.DataFrame()
    # excluUseAr 비율 조정
    df['excluUseAr'] = df['excluUseAr'].astype(str).str.replace(',', '').astype(float)
    df['excluUseAr_adj'] = df['excluUseAr'] * 121 / 400
    df['dealYear'] = df['dealYear'].astype(int)

    # 피벗 생성
    grouped = df.groupby(['umdNm','aptNm','dealYear'], dropna=False)
    agg = grouped.agg(
        case_count = ('dealYear', 'size'),
        avg_value  = (value_col, 'mean'),
        avg_exclu  = ('excluUseAr_adj', 'mean')
    ).reset_index()
    # 컬럼 순서: 년도별 [count, value, exclu] 반복
    years = sorted(agg['dealYear'].unique())
    new_cols = []
    for y in years:
        new_cols += [
            f"case_count_{y}",
            f"avg_value_{y}",
            f"avg_exclu_{y}"
        ]
    pivot = agg.pivot_table(
        index=['umdNm','aptNm'], columns='dealYear',
        values=['case_count','avg_value','avg_exclu']
    )
    # 컬럼 레벨 정리
    pivot.columns = new_cols
    pivot = pivot.reset_index()

    # 포맷팅: 수치형 컬럼으로 변환 & 천단위 콤마·반올림
    for c in pivot.columns:
        if c.startswith('case_count_'):
            pivot[c] = pivot[c].astype(int)
        elif c.startswith('avg_exclu_'):
            pivot[c] = pivot[c].round(2)
        else:
            # avg_value, dealAmount, fixed_deposit 등
            pivot[c] = pivot[c].map(lambda x: f"{x:,.2f}" if pd.notna(x) else "")

    return pivot

# ── 8) 엑셀로 저장 ──
with pd.ExcelWriter(args.output, engine='xlsxwriter') as writer:
    df_sale.to_excel(writer, sheet_name='매매(Raw)', index=False)
    df_rent.to_excel(writer, sheet_name='전월세(Raw)', index=False)
    df_silv.to_excel(writer, sheet_name='분양권(Raw)', index=False)

    sale_pv = make_pivot(df_sale, 'dealAmount', '매매(수정)')
    rent_pv = make_pivot(df_rent, 'fixed_deposit', '전세(수정)')
    silv_pv = make_pivot(df_silv, 'dealAmount', '분양권(수정)')

    if not sale_pv.empty:
        sale_pv.to_excel(writer, sheet_name='매매(수정)', index=False)
    if not rent_pv.empty:
        rent_pv.to_excel(writer, sheet_name='전세(수정)', index=False)
    if not silv_pv.empty:
        silv_pv.to_excel(writer, sheet_name='분양권(수정)', index=False)

print(f"리포트가 '{args.output}' 로 저장되었습니다.")
