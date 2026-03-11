# 배전계통 OLTC-ESS 협조 제어 동적 시뮬레이터

이 프로젝트는 `pandapower` 기반 전력계통 해석과 `streamlit` 기반 UI를 결합한 배전계통 시뮬레이터입니다. 154kV/22.9kV 배전계통에서 OLTC 1대와 ESS 1대를 중심으로 전압 유지와 선로용량 제약을 함께 검토할 수 있도록 고도화했습니다.

## 핵심 목적
- 부하 및 재생에너지 변동에 따른 전압 변화 분석
- OLTC-ESS 협조제어 알고리즘 시험
- 자동 민감도 분석을 통한 허용치 이탈 지점 탐색
- 분석 결과의 Excel/Word 보고서 생성

## 현재 파일 구성
- `DL.py`: Streamlit UI, 실행 상태 관리, 결과 시각화
- `sim_engine.py`: 기본 계통 모델, 시간패턴 보간, 공통 시뮬레이션 유틸리티
- `coordinated_engine.py`: 협조제어 로직, 자동 민감도 분석, 보고서 생성
- `limit_finder.py`: CLI 기반 자동 분석 실행 스크립트
- `walkthrough.md`: 이번 고도화 작업 상세 내역
- `project_summary_2026-03-10.md`: 현재까지의 전체 작업 요약 및 인수인계 문서

## 주요 기능

### 1. 계통 모델링
- 154kV / 22.9kV 변전소 구조 반영
- OLTC 1대, ESS 1대 기준 모델
- 버스별 Load / PV / Wind 설정 가능
- 선로 길이와 임피던스 설정 가능

### 2. 24시간 시계열 시뮬레이션
- 부하, PV, Wind 시간패턴 입력 및 업로드
- 1분 또는 10분 등 시간 간격 설정 가능
- 전압, 선로용량, ESS SOC, ESS 출력, OLTC 탭 변화 추적

### 3. 협조제어 알고리즘
- 전압 허용범위: 기본 `0.94 ~ 1.06 p.u.`
- 선로용량 제한: 기본 `12 MVA`
- 상태기반 제어: `선로 혼잡 > 전압 이상`
- 전압 제어 시 OLTC 우선, ESS 보조
- ESS 효율 파라미터를 SOC 계산에 반영
- 알고리즘 탭에서 주요 파라미터 수정 가능

### 4. 자동 민감도 분석
다음 세 가지 시나리오를 선택할 수 있습니다.
- `부하만 증가`
- `재생에너지 출력 증가`
- `부하구간별 재생에너지 증가`

자동 민감도 분석은 시작 배율부터 최대 배율까지 단계별로 반복 시뮬레이션을 수행하며, 각 회차에 대해 다음을 계산합니다.
- 최소/최대 전압
- 최대 선로용량
- 부하/PV/WT 총합 범위
- ESS 출력 범위
- ESS SOC 범위
- OLTC 탭 범위
- 허용치 만족 여부

### 5. 보고서 및 결과물
- 단일 시나리오 결과 Excel 다운로드
- 자동 민감도 분석 결과 Word 보고서 생성
- 보고서에는 다음을 포함하도록 구성
  - 계통 구성
  - 부하/재생에너지 패턴 그래프
  - 민감도 종합 그래프
  - 대표 시뮬레이션 그래프
  - 회차별 전압 및 선로용량 그래프
  - 회차별 운영 범위
  - 이상 결과 원인 분석 및 개선 방향

## 실행 방법

### Streamlit UI 실행
```bash
streamlit run DL.py
```

### 직접 실행
```bash
python DL.py
```

### CLI 자동 분석 실행
```bash
python limit_finder.py
```

예시:
```bash
python limit_finder.py --scenario renewable_increase --start-scale 1.0 --step 0.1 --max-scale 3.0
```

## 권장 패키지 설치
```bash
pip install streamlit pandapower pandas numpy plotly openpyxl xlsxwriter matplotlib python-docx
```

## 현재 알려진 제약
- Streamlit 페이지 내부 버튼으로 실행 중 루프를 즉시 중단하는 구조는 아직 아님
- 실제 강제 중지는 우상단 `Stop` 버튼 사용
- 보고서 그래프 생성은 환경에 따라 `matplotlib` 설치가 필요함
- 일부 환경에서는 `python-docx` 경로가 실패할 수 있어 fallback 경로를 함께 두고 있음

## 참고 문서
- [walkthrough.md](./walkthrough.md)
- [project_summary_2026-03-10.md](./project_summary_2026-03-10.md)
