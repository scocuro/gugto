# common.py
import csv, os, requests

def get_region_code(region_name, code_csv='code_raw.csv'):
    """
    code_raw.csv 에서 시군구 이름에 대응하는 코드를 찾아 반환합니다.
    """
    here = os.path.dirname(__file__)
    path = os.path.join(here, code_csv)
    with open(path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['시군구명'] == region_name:
                return row['시군구코드']
    raise ValueError(f"'{region_name}' 코드를 못 찾았습니다.")

def fetch_json_list(start, end, region_code, api_key):
    """
    미분양 API URL 을 만들어 요청하고, result_data.formList 리턴.
    """
    url = (
      f"http://stat.molit.go.kr/portal/openapi/service/rest/getList.do"
      f"?key={api_key}"
      f"&form_id=5328&style_num=1"
      f"&start_dt={start}&end_dt={end}"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    js = resp.json()
    return js['result_data']['formList']