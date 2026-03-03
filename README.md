# ⚡ 24시간 동적 시뮬레이션: 협조 제어 시뮬레이터 (OLTC & ESS)

본 프로젝트는 **배전계통의 전압 문제를 해결하기 위해 변전소의 OLTC(On-Load Tap Changer)와 말단 모선의 ESS(Energy Storage System) 간의 협조 제어 알고리즘**을 시뮬레이션하는 도구입니다. Python 기반의 전력계통 해석 라이브러리인 `pandapower`와 웹 대시보드 프레임워크인 `streamlit`을 활용하여 구축되었습니다.

---

## 🚀 주요 기능 (Features)

1. **배전계통 네트워크 모델링**
   - 154kV / 22.9kV 변전소 모델링 및 OLTC 탭 제어(Tap Position 제어).
   - CNCV 및 ACSR 케이블 등 선로별 파라미터(길이, 임피던스) 사용자 설정 인터페이스.
   - 재생에너지(PV, Wind), 부하(Load), ESS 등의 노드별 분산 전원 투입 설정.

2. **24시간 시계열 동적 시뮬레이션 (분 단위 보간)**
   - 사용자가 입력한 24시간 스케줄(Load, PV, Wind 패턴)을 1분 단위로 선형 보간(Linear Interpolation)하여 세밀한 조류해석 수행.
   - 엑셀(.xlsx) 또는 CSV 파일을 업로드하여 사용자 정의 패턴 로드 가능.

3. **협조 제어 알고리즘 (Coordinated Control)**
   - **ESS 단기 응답 제어**: 전압 초과/미달 시 ESS가 즉각적으로 충·방전하여 전압 변동을 완화 (SOC 제한 고려).
   - **OLTC 장기 응답 제어**: 전압 문제가 일정 시간(시지연, Time Delay) 지속될 경우 OLTC 탭을 조절하여 전체 계통 전압을 리셋.

4. **실시간 시각화 대시보드 (Visualization)**
   - 조류해석이 진행되는 동안 진행 상태(Progress) 인터페이스 제공.
   - 24시간 모선별 전압 프로파일(p.u.), OLTC 탭 동작 횟수, ESS의 SOC 변화 및 충방전 출력(MW)을 `plotly` 차트로 직관적 시각화.

---

## 🛠️ 설치 및 요구사항 (Prerequisites)

본 프로젝트를 실행하기 위해 Python 3.8 이상의 환경이 권장됩니다.
필요한 패키지는 `requirements.txt`에 명시되어 있으며, 추가적으로 아래의 라이브러리들이 필요합니다.

```bash
# 필수 라이브러리 설치
pip install streamlit pandapower pandas numpy plotly openpyxl
```

---

## 💻 실행 방법 (Usage)

일반적인 파이썬 실행 명령어(`python DL.py`)가 아닌, **Streamlit 전용 명령어**를 사용하여 실행해야 대시보드(웹 UI)를 확인할 수 있습니다.

```bash
streamlit run DL.py
```

명령어를 실행하면 자동으로 기본 웹 브라우저가 열리며 대시보드 화면이 나타납니다. 만약 창이 열리지 않는다면 터미널에 출력된 `Local URL` (예: `http://localhost:8501`)을 브라우저 주소창에 직접 입력하세요.

---

## 📂 파일 구조 및 핵심 로직 (Structure)

- `DL.py`: 메인 애플리케이션 파일입니다.
  - **사이드바 구성**: 선로 길이, 임피던스(R+jX), OLTC 설정(민감도, 시지연 등), ESS 제어 설정 입력.
  - **메인 화면 입력부**: DataFrame Editor를 통해 버스별 용량(MW, MVar) 및 24시간 출력 패턴(%)을 입력.
  - **`create_dynamic_network()`**: 입력된 파라미터를 기반으로 `pandapower` 빈 네트워크(empt network)에 Bus, Line, Load, sgen, storage 등을 동적으로 생성.
  - **Simulation Loop (1440분)**:
    1. 각 시간대별 부하 및 재생에너지 출력 업데이트.
    2. 조류해석(`runpp`) 1차 수행 (수렴 실패 시 기본 전압 1.0 보장).
    3. 전압 제한 위반 시 ESS 출력 산정 및 SOC 갱신.
    4. 조류해석 2차 수행 (ESS 개입 후).
    5. 말단 전압을 확인하여 OLTC Time Delay 타이머 동작 및 탭 변환.
  - **결과 플로팅**: 시뮬레이션 종료 후 수집된 데이터를 바탕으로 결과 그래프 출력.

---

## 📝 향후 업데이트 계획 (TODO)

- [ ] 시뮬레이션 결과(전압 프로파일, ESS 동작 내역 등)를 엑셀 파일로 추출(Download)하는 기능 추가.
- [ ] 다중 변전소 혹은 복조 계통(Meshed Network) 구조 지원 확장.
- [ ] 조류해석 속도 개선을 위한 Numba 활성화 테스트 최적화.

---
**Author**: [개발자 이름/아이디를 입력하세요]
**License**: MIT License (또는 적용할 라이선스)

---

## 📖 상세 업데이트 및 검증 가이드
최신 업데이트 내역, 에러 수정 사항 및 시뮬레이션 검증 결과에 대한 상세 내용은 아래 문서를 참조하세요.

👉 [상세 워크스루(Walkthrough) 바로가기](./docs/walkthrough.md)