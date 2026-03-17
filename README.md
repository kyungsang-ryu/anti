# 배전계통 OLTC-ESS 협조 제어 동적 시뮬레이터

이 프로젝트는 `pandapower` 기반 전력계통 해석과 `streamlit` 기반 UI를 결합한 배전계통 시뮬레이터입니다. 154kV/22.9kV 배전계통에서 OLTC 1대와 ESS 1대를 중심으로 전압 유지와 선로용량 제약을 함께 검토할 수 있도록 고도화했습니다.

## 핵심 목적
- 부하 및 재생에너지 변동에 따른 전압 변화 분석
- OLTC-ESS 협조제어 알고리즘 시험
- 자동 민감도 분석을 통한 허용치 이탈 지점 탐색
- 연구 질문 중심 배치 시나리오 생성 및 병렬 실행
- 분석 결과의 Excel/Word 보고서 생성

## 현재 파일 구성
- `DL.py`: Streamlit UI, 실행 상태 관리, 결과 시각화, 배치 시나리오 preview/실행 연결
- `sim_engine.py`: 기본 계통 모델, 시간패턴 보간, 공통 시뮬레이션 유틸리티
- `coordinated_engine.py`: 협조제어 로직, 자동 민감도 분석, 연구형 시나리오 생성, 배치 실행, 보고서 생성
- `limit_finder.py`: CLI 기반 자동 분석 및 배치 시나리오 실행 스크립트
- `docs/walkthrough.md`: 작업 내역과 구조 변경 기록
- `project_summary_2026-03-10.md`: 전체 작업 요약 및 인수인계 문서

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
- 제어 파라미터를 UI에서 수정 가능

### 4. 자동 민감도 분석
다음 시나리오를 반복 탐색할 수 있습니다.
- `부하만 증가`
- `재생에너지 출력 증가`
- `부하와 재생에너지 동시 증가`
- `부하구간별 재생에너지 증가`

자동 민감도 분석은 시작 배율부터 최대 배율까지 단계적으로 반복 시뮬레이션을 수행하며, 각 회차에 대해 다음을 계산합니다.
- 최소/최대 전압
- 최대 선로용량
- 부하/PV/WT 총합 범위
- ESS 출력 범위
- ESS SOC 범위
- OLTC 탭 범위
- 허용치 만족 여부

### 5. 연구형 배치 시나리오 생성
배치 시나리오는 무작위 조합이 아니라 연구 질문별 `scenario mode`로 생성됩니다.

지원 mode:
- `hosting_capacity`
- `load_pv_map`
- `ess_sizing`

공통 생성 흐름:
- 기준 설정 확정
- mode별 가변 변수 선택
- 유효성 검사 및 중복 제거
- `SCN_001`, `SCN_002` 형식의 시나리오 ID 부여
- UI preview 표시
- 시나리오별 독립 실행
- summary CSV / Excel 집계

mode별 의미:
- `hosting_capacity`: 부하와 ESS 조건을 고정하고 PV를 단조 증가시켜 수용 한계와 첫 위반 지점을 찾음
- `load_pv_map`: Load-PV 운전점을 구조화해 2D 운영영역을 작성함
- `ess_sizing`: 대표 스트레스 케이스를 고정하고 ESS 크기와 위치를 바꿔 최소 필요 용량과 위치 민감도를 평가함

예시 진행:
- `hosting_capacity`: `SCN_001 PV 0.80 -> SCN_002 PV 1.00 -> SCN_003 PV 1.20`
- `load_pv_map`: `SCN_001 Load 0.90 / PV 0.80, SCN_002 Load 0.90 / PV 1.20, SCN_003 Load 1.10 / PV 0.80, SCN_004 Load 1.10 / PV 1.20`
- `ess_sizing`: `SCN_001 Size 0.00 / Bus 5 -> SCN_002 Size 0.50 / Bus 5 -> SCN_003 Size 1.00 / Bus 5`

### 6. 보고서 및 결과물
- 단일 시나리오 결과 Excel 다운로드
- 자동 민감도 분석 결과 Word 보고서 생성
- 배치 시나리오 summary CSV / Excel 다운로드
- Word 보고서에는 시나리오 진행 원칙과 예시 흐름도 함께 포함

## 최근 업데이트 (데이터 관리/연구 분석 고도화)
### 1. 데이터 관리 효율화
- 다중 시나리오 결과를 회차별 시트로 통합한 상세 Excel 다운로드를 지원
- 배치 실행에서도 요약 CSV/Excel + 상세 로그 Excel을 함께 생성
- `st.data_editor` 수정값을 세션에 즉시 반영하도록 동기화해 입력 유실 문제 완화
- 모선 수를 증감해도 기존 Load/PV/Wind/ESS 관련 값이 유지되도록 리사이즈 로직 개선

### 2. 연구 목적형 배치 분석 강화
- 무작위 조합 대신 연구 질문 중심 `scenario mode` 생성 유지
- `hosting_capacity`, `load_pv_map`, `ess_sizing` 모드를 통해 목적성 있는 시나리오 생성
- 배치 실행 전 시나리오 preview와 유효성 검증을 통해 비현실/중복 케이스를 제거

### 3. 설정/편집 UX 안정화
- ESS 설치 버스 선택이 다시 기본값으로 되돌아가던 문제 수정
- 모선 개수 변경 후 5로 복귀하던 상태 동기화 문제 수정
- 권장 기본값 적용 시 Streamlit widget state 충돌이 나지 않도록 `pending_cfg_update` 경로로 적용
- 창 재시작 후에도 사이드바 설정 + `bus_df` + `time_df`를 복원하도록 저장/복원 로직 확장

### 4. 보고서 품질 개선
- 회차별(run-by-run) 섹션에 ESS SOC 시계열 그래프 추가
- 통합 오버레이 섹션에 ESS SOC 오버레이 그래프 추가
- 샘플 보고서 형식과 유사한 그래프 포함 fallback DOCX 경로를 우선 사용

### 5. 전압 그래프 왜곡(찌그러짐) 완화
- 기존 `min/max envelope` 표시에서 발생하던 버스 전환 artifact를 줄이기 위해,
  대표 저전압/고전압 버스 연속 시계열 기준으로 보고서 그래프를 재구성
- ESS 명령값을 목표치로 즉시 점프시키지 않고 ramp 기반으로 추종시키는 방식으로 완화해
  고배율 구간 파형의 급격한 꺾임을 줄임

## 실행 방법

### Streamlit UI 실행
```bash
streamlit run DL.py
```

### 직접 실행
```bash
python DL.py
```

### CLI 자동 민감도 분석 실행
```bash
python limit_finder.py
```

예시:
```bash
python limit_finder.py --scenario renewable_increase --start-scale 1.0 --step 0.1 --max-scale 3.0
```

### CLI 배치 시나리오 실행
`hosting_capacity` 예시:
```bash
python limit_finder.py --batch-mode --batch-scenario-mode hosting_capacity --pv-penetration 0.8,1.0,1.2,1.4,1.6 --load-growth 1.0 --control-case oltc_ess
```

`load_pv_map` 예시:
```bash
python limit_finder.py --batch-mode --batch-scenario-mode load_pv_map --pv-penetration 0.8,1.0,1.2 --load-growth 0.8,1.0,1.2 --control-case oltc_only
```

`ess_sizing` 예시:
```bash
python limit_finder.py --batch-mode --batch-scenario-mode ess_sizing --base-pv-penetration 1.6 --base-load-growth 1.0 --ess-size 0.0,0.5,1.0,1.5 --ess-location 3,4,5
```

## 권장 패키지 설치
```bash
pip install streamlit pandapower pandas numpy plotly openpyxl xlsxwriter matplotlib python-docx
```

## 현재 알려진 제약
- 기존 Streamlit UI 구조는 유지하며, batch 기능은 별도 페이지가 아니라 기존 expander 안에 최소한으로 추가됨
- `load_pv_map`만 연구 목적상 구조화된 2D 조합을 허용하며, 그 외 mode에서는 의미 없는 Cartesian product를 만들지 않음
- 배치 실행은 시나리오 단위로 독립적이어서 병렬 실행이 가능함
- 상세 시계열 저장을 켜면 프로세스 간 전송 부담 때문에 serial fallback을 사용함
- Streamlit 페이지 내부 버튼으로 실행 중 루프를 즉시 중단하는 구조는 아직 아님
- 실제 강제 중지는 우상단 `Stop` 버튼 사용
- 보고서 그래프 생성은 환경에 따라 `matplotlib` 설치가 필요함
- 일부 환경에서는 `python-docx` 경로가 실패할 수 있어 fallback 경로를 함께 둠

## 참고 문서
- [docs/walkthrough.md](./docs/walkthrough.md)
- [project_summary_2026-03-10.md](./project_summary_2026-03-10.md)
