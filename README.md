# 공공데이터 실거래 리포트

## 개요
- 지역, 기간, 준공연도, 전용면적 조건으로 공공데이터 API를 호출
- 매매/전월세/분양권 거래를 연도별로 집계
- 엑셀 파일로 결과 저장

## 로컬 실행
1. Python 3 설치 확인
2. 가상환경 생성 및 활성화:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: .\venv\Scripts\activate.bat
   ```
3. 의존성 설치:
   ```bash
   pip install -r requirements.txt
   ```
4. API 키 환경변수 설정:
   ```bash
   export PUBLIC_DATA_API_KEY="발급받은_API_키"  # Windows PowerShell에서 Set-ExecutionPolicy 필요
   ```
5. 스크립트 실행:
   ```bash
   python real_estate_report.py \
     --region "천안시 동남구" \
     --start-year 2020 \
     --built-after 2015 \
     --area-min 70 \
     --area-max 80 \
     --output cheonan_report.xlsx
   ```

## GitHub Actions
- `.github/workflows/real_estate_report.yml` 참조
