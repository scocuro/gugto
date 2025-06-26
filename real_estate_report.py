# real_estate_report.py
import os
import requests
import pandas as pd
from datetime import datetime

# ---------- 사용자 입력 받기 (인터랙티브) ----------
print("공공데이터 실거래 리포트 생성기")
region_code = input("1) 지역 법정동코드(LAWD_CD)를 입력하세요 (예: 천안시 동남구 코드): ")
start_year = int(input("2) 조회 시작 연도(YYYY) 입력하세요 (예: 2020): "))
built_after = int(input("3) 준공 연도 이후 입력하세요 (예: 2015): "))
area_min = float(input("4) 최소 전용면적(㎡) 입력하세요 (예: 70): "))
area_max = float(input("5) 최대 전용면적(㎡) 입력하세요 (예: 80): "))
output_file = input("6) 저장할 엑셀 파일명 입력하세요 (예: report.xlsx): ") or "report.xlsx"

# 환경변수로 관리되는 API 키 가져오기
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
if not API_KEY:
    raise RuntimeError("환경변수 PUBLIC_DATA_API_KEY에 API 키를 설정하세요.")

# 실제 공공데이터 포털에서 받은 엔드포인트로 교체하세요
BASE_URL = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'

# ----------------------------------------------------------------------------
# 1. 데이터 수집: 연도별·월별로 API 호출
# ----------------------------------------------------------------------------
def fetch_transactions(lawd_cd: str, deal_ym: str, page_no: int, num_of_rows: int = 1000) -> pd.DataFrame:
    params = {
        'serviceKey': API_KEY,
        'LAWD_CD': lawd_cd,
        'DEAL_YMD': deal_ym,
        'pageNo': page_no,
        'numOfRows': num_of_rows,
    }
    resp = requests.get(BASE_URL, params=params)
    # XML 응답일 경우 pandas.read_xml 등으로 파싱 필요
    data = resp.json().get('items', []) if 'json' in resp.headers.get('Content-Type','') else []
    return pd.DataFrame(data)

# ----------------------------------------------------------------------------
# 2. 전체 기간 데이터 조회
# ----------------------------------------------------------------------------
records = []
current_year = datetime.now().year
for year in range(start_year, current_year + 1):
    for month in range(1, 13):
        ymd = f"{year}{month:02d}"
        page = 1
        while True:
            df = fetch_transactions(region_code, ymd, page)
            if df.empty:
                break
            # 필터링: 건설 연도, 면적 조건 적용
            df = df[df['건축년도'].astype(int) >= built_after]
            df = df[(df['전용면적'].astype(float) >= area_min) & (df['전용면적'].astype(float) <= area_max)]
            if df.empty:
                break
            records.append(df)
            page += 1

if not records:
    raise RuntimeError("조건에 맞는 거래 데이터가 없습니다.")
all_data = pd.concat(records, ignore_index=True)

# ----------------------------------------------------------------------------
# 3. 가공 및 집계
# ----------------------------------------------------------------------------
all_data['거래년월'] = pd.to_datetime(all_data['년월'], format='%Y%m')
all_data['year'] = all_data['거래년월'].dt.year
all_data['unit_price'] = all_data['거래금액'].astype(float) / all_data['전용면적'].astype(float)

agg = (
    all_data
    .groupby(['법정동','단지명','year'])
    .agg(
        avg_price=('거래금액','mean'),
        count=('거래금액','size'),
        avg_unit_price=('unit_price','mean')
    )
    .reset_index()
)

# ----------------------------------------------------------------------------
# 4. 엑셀 저장
# ----------------------------------------------------------------------------
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    agg.to_excel(writer, sheet_name='연도별집계', index=False)

print(f"리포트가 {output_file} 로 저장되었습니다.")
