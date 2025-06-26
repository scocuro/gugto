# real_estate_report.py
import argparse
import os
import requests
import pandas as pd
from datetime import datetime

# API 키는 환경변수에서 가져옵니다
API_KEY = os.getenv('PUBLIC_DATA_API_KEY')
BASE_URL = 'https://api.realestate.go.kr/transactions'  # 실제 엔드포인트로 교체

def fetch_transactions(region, start_year, built_after, area_min, area_max, txn_type):
    records = []
    page = 1
    while True:
        params = {
            'serviceKey': API_KEY,
            'region': region,
            'txnType': txn_type,
            'builtAfter': built_after,
            'areaMin': area_min,
            'areaMax': area_max,
            'yearFrom': start_year,
            'page': page,
            'perPage': 1000,
        }
        resp = requests.get(BASE_URL, params=params)
        data = resp.json().get('items', [])
        if not data:
            break
        records.extend(data)
        page += 1
    return pd.DataFrame(records)

def process_and_aggregate(df):
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['unit_price'] = df['price'] / df['area']
    agg = (
        df
        .groupby(['region','complex','year'])
        .agg(
            avg_price=('price','mean'),
            count=('price','size'),
            avg_unit_price=('unit_price','mean')
        )
        .reset_index()
    )
    return agg

def save_to_excel(dfs, output_path):
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        for sheet_name, df in dfs.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--region', required=True, help='지역 (예: 천안시 동남구)')
    parser.add_argument('--start-year', type=int, default=2020, help='시작 연도')
    parser.add_argument('--built-after', type=int, default=2015, help='준공 연도 이후')
    parser.add_argument('--area-min', type=float, default=70.0, help='최소 전용면적')
    parser.add_argument('--area-max', type=float, default=80.0, help='최대 전용면적')
    parser.add_argument('--output', default='report.xlsx', help='출력 엑셀 파일명')
    args = parser.parse_args()

    dfs = {}
    for txn_type, label in [('sale','매매'), ('rent','전월세'), ('presale','분양권')]:
        raw = fetch_transactions(
            args.region, args.start_year, args.built_after,
            args.area_min, args.area_max, txn_type
        )
        dfs[label] = process_and_aggregate(raw)

    save_to_excel(dfs, args.output)
    print(f"Report saved to {args.output}")

if __name__ == '__main__':
    main()
