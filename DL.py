import math
import os
import subprocess
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

from coordinated_engine import (
    CONTROL_CASE_NO_CONTROL,
    CONTROL_CASE_OLTC_ESS,
    CONTROL_CASE_OLTC_ONLY,
    SCENARIO_BOTH_INCREASE,
    SCENARIO_LOAD_INCREASE,
    SCENARIO_MODE_ESS_SIZING,
    SCENARIO_MODE_HOSTING_CAPACITY,
    SCENARIO_MODE_LOAD_PV_MAP,
    SCENARIO_RENEWABLE_BY_LOAD_LEVEL,
    SCENARIO_RENEWABLE_INCREASE,
    build_analysis_excel_bytes,
    build_batch_summary_csv_bytes,
    build_batch_summary_excel_bytes,
    build_scenario_preview_df,
    build_word_report_bytes,
    control_case_label,
    generate_scenarios,
    prepare_single_ess_bus_df,
    run_batch_simulations,
    run_sensitivity_search,
    run_single_simulation,
    scenario_label,
    scenario_mode_label,
    scenario_mode_metadata,
)
from sim_engine import (
    BUS_COL,
    LOAD_P_COL,
    LOAD_Q_COL,
    PV_P_COL,
    WIND_P_COL,
    default_bus_dataframe,
    default_config,
    default_time_profile_dataframe,
)

st.set_page_config(page_title="배전계통 OLTC-ESS 협조 제어 시뮬레이터", layout="wide")
st.title("배전계통 OLTC-ESS 협조 제어 시뮬레이터")

SCENARIO_OPTIONS = [
    SCENARIO_LOAD_INCREASE,
    SCENARIO_RENEWABLE_INCREASE,
    SCENARIO_BOTH_INCREASE,
    SCENARIO_RENEWABLE_BY_LOAD_LEVEL,
]
BUS_OPTIONS = [f"Bus {i}" for i in range(1, 6)]
BUS_INDEX_MAP = {name: idx + 1 for idx, name in enumerate(BUS_OPTIONS)}
BATCH_SCENARIO_MODE_OPTIONS = [
    SCENARIO_MODE_HOSTING_CAPACITY,
    SCENARIO_MODE_LOAD_PV_MAP,
    SCENARIO_MODE_ESS_SIZING,
]
CONTROL_CASE_OPTIONS = [
    CONTROL_CASE_NO_CONTROL,
    CONTROL_CASE_OLTC_ONLY,
    CONTROL_CASE_OLTC_ESS,
]
def algorithm_preset_values(preset_name: str) -> dict:
    # 문서 기준 알고리즘과 현재 기본 알고리즘을 버튼으로 바로 적용할 수 있도록 preset을 제공한다.
    presets = {
        "baseline": {
            "cfg_voltage_min_limit": 0.94,
            "cfg_voltage_max_limit": 1.06,
            "cfg_voltage_low_off": 0.955,
            "cfg_voltage_high_off": 1.045,
            "cfg_line_limit_mva": 12.0,
            "cfg_line_return_ratio": 0.95,
            "cfg_oltc_step": 1.25,
            "cfg_oltc_delay_mins": 3.0,
            "cfg_oltc_return_delay_mins": 20.0,
            "cfg_ess_min_soc": 10.0,
            "cfg_ess_max_soc": 90.0,
            "cfg_ess_p_gain": 16.0,
            "cfg_ess_q_gain": 8.0,
            "cfg_line_relief_gain": 3.0,
            "cfg_ess_ramp_rate_mw_per_min": 0.2,
        },
        "report_recommended": {
            "cfg_voltage_min_limit": 0.94,
            "cfg_voltage_max_limit": 1.06,
            "cfg_voltage_low_off": 0.955,
            "cfg_voltage_high_off": 1.045,
            "cfg_line_limit_mva": 12.0,
            "cfg_line_return_ratio": 0.95,
            "cfg_oltc_step": 1.25,
            "cfg_oltc_delay_mins": 3.0,
            "cfg_oltc_return_delay_mins": 20.0,
            "cfg_ess_min_soc": 10.0,
            "cfg_ess_max_soc": 90.0,
            "cfg_ess_p_gain": 18.0,
            "cfg_ess_q_gain": 10.0,
            "cfg_line_relief_gain": 3.5,
            "cfg_ess_ramp_rate_mw_per_min": 0.2,
        },
    }
    return presets[preset_name].copy()


def queue_algorithm_preset(preset_name: str):
    st.session_state["pending_cfg_update"] = algorithm_preset_values(preset_name)
    st.rerun()


def parse_impedance(z_str: str):
    try:
        z_str = z_str.replace(" ", "").lower()
        parts = z_str.split("+")
        r = float(parts[0])
        x = float(parts[1].replace("j", "").replace("i", ""))
        return r, x
    except Exception:
        st.error(f"임피던스 입력 형식 오류 ({z_str}). 'R + jX' 형식으로 입력하세요.")
        return 0.0, 0.0


def init_session_state():
    defaults = default_config()
    st.session_state.setdefault("bus_df", default_bus_dataframe())
    st.session_state.setdefault("time_df", default_time_profile_dataframe())
    st.session_state.setdefault("sim_results", None)
    st.session_state.setdefault("sim_events", None)
    st.session_state.setdefault("sim_result_context", None)
    st.session_state.setdefault("is_running", False)
    st.session_state.setdefault("run_request", None)
    st.session_state.setdefault("active_run_action", None)
    st.session_state.setdefault("last_run_action", None)
    st.session_state.setdefault("interrupted_run_action", None)
    st.session_state.setdefault("excel_bytes", None)
    st.session_state.setdefault("uploaded_profile_signature", None)
    st.session_state.setdefault("auto_search_result", None)
    st.session_state.setdefault("auto_report_bytes", None)
    st.session_state.setdefault("auto_report_name", "simulation_report.docx")
    st.session_state.setdefault("batch_result", None)
    st.session_state.setdefault("batch_summary_csv_bytes", None)
    st.session_state.setdefault("batch_summary_excel_bytes", None)
    st.session_state.setdefault("batch_summary_prefix", "batch_summary")

    for key, value in defaults.items():
        st.session_state.setdefault(f"cfg_{key}", value)

    st.session_state.setdefault("cfg_scenario", SCENARIO_LOAD_INCREASE)
    st.session_state.setdefault("cfg_auto_start_scale", 1.0)
    st.session_state.setdefault("cfg_auto_step", 0.1)
    st.session_state.setdefault("cfg_auto_max_scale", 3.0)
    st.session_state.setdefault("cfg_voltage_min_limit", 0.94)
    st.session_state.setdefault("cfg_voltage_max_limit", 1.06)
    st.session_state.setdefault("cfg_voltage_low_off", 0.955)
    st.session_state.setdefault("cfg_voltage_high_off", 1.045)
    st.session_state.setdefault("cfg_line_limit_mva", 12.0)
    st.session_state.setdefault("cfg_line_return_ratio", 0.95)
    st.session_state.setdefault("cfg_oltc_return_delay_mins", 20)
    st.session_state.setdefault("cfg_ess_min_soc", 10.0)
    st.session_state.setdefault("cfg_ess_max_soc", 90.0)
    st.session_state.setdefault("cfg_ess_p_gain", 16.0)
    st.session_state.setdefault("cfg_ess_q_gain", 8.0)
    st.session_state.setdefault("cfg_line_relief_gain", 3.0)
    st.session_state.setdefault("cfg_ess_ramp_rate_mw_per_min", 0.2)
    st.session_state.setdefault("cfg_low_load_upper_pct", 60.0)
    st.session_state.setdefault("cfg_mid_load_upper_pct", 85.0)
    st.session_state.setdefault("cfg_ess_bus_number", 5)
    st.session_state.setdefault("cfg_ess_power_mw", 5.0)
    st.session_state.setdefault("cfg_ess_capacity_mwh", 15.0)
    st.session_state.setdefault("cfg_z_cncv_str", "0.07 + j0.12")
    st.session_state.setdefault("cfg_z_acsr_str", "0.18 + j0.39")
    st.session_state.setdefault("cfg_ess_bus_label", "Bus 5")
    st.session_state.setdefault("cfg_batch_mode", SCENARIO_MODE_HOSTING_CAPACITY)
    st.session_state.setdefault("cfg_batch_control_case", CONTROL_CASE_OLTC_ESS)
    st.session_state.setdefault("cfg_batch_pv_values", "0.8,1.0,1.2,1.4,1.6")
    st.session_state.setdefault("cfg_batch_ess_values", "0.0,0.5,1.0,1.5")
    st.session_state.setdefault("cfg_batch_load_values", "0.8,1.0,1.2,1.4")
    st.session_state.setdefault("cfg_batch_ess_location_values", "3,4,5")
    st.session_state.setdefault("cfg_batch_hosting_load_growth", 1.0)
    st.session_state.setdefault("cfg_batch_base_pv_penetration", 1.6)
    st.session_state.setdefault("cfg_batch_base_load_growth", 1.0)
    st.session_state.setdefault("cfg_batch_parallel", True)
    st.session_state.setdefault("cfg_batch_include_timeseries", False)
    st.session_state.setdefault("cfg_batch_max_workers", min(4, max(1, int(os.cpu_count() or 1))))


def apply_pending_config_updates():
    pending = st.session_state.pop("pending_cfg_update", None)
    if pending:
        for key, value in pending.items():
            st.session_state[key] = value


def recover_interrupted_run_state():
    # 우상단 Stop으로 중단된 경우 다음 rerun에서 자동 실행 루프에 빠지지 않도록 실행 상태를 복구한다.
    if st.session_state.get("is_running") and st.session_state.get("active_run_action") and not st.session_state.get("run_request"):
        st.session_state["interrupted_run_action"] = st.session_state.get("active_run_action")
        st.session_state["is_running"] = False
        st.session_state["active_run_action"] = None


def queue_run(action: str):
    # 버튼 클릭 시 실제 계산은 다음 rerun에서 시작한다.
    st.session_state["run_request"] = action
    st.session_state["last_run_action"] = action
    st.session_state["interrupted_run_action"] = None
    st.rerun()


def reset_run_state(clear_results: bool = True):
    # 중단 후 복구용 초기화: 입력값은 유지하고 실행 상태/결과만 정리한다.
    st.session_state["is_running"] = False
    st.session_state["run_request"] = None
    st.session_state["active_run_action"] = None
    st.session_state["interrupted_run_action"] = None
    if clear_results:
        st.session_state["sim_results"] = None
        st.session_state["sim_events"] = None
        st.session_state["sim_result_context"] = None
        st.session_state["excel_bytes"] = None
        st.session_state["auto_search_result"] = None
        st.session_state["auto_report_bytes"] = None
        st.session_state["batch_result"] = None
        st.session_state["batch_summary_csv_bytes"] = None
        st.session_state["batch_summary_excel_bytes"] = None



def build_batch_settings_from_state() -> dict:
    """Collect research-question-driven batch settings while keeping the current UI layout intact."""
    mode = st.session_state["cfg_batch_mode"]
    settings = {
        "mode": mode,
        "default_ess_location": int(st.session_state["cfg_ess_bus_number"]),
        "base_ess_power_mw": float(st.session_state["cfg_ess_power_mw"]),
        "base_ess_capacity_mwh": float(st.session_state["cfg_ess_capacity_mwh"]),
    }
    if mode == SCENARIO_MODE_HOSTING_CAPACITY:
        settings.update(
            {
                "pv_penetration": st.session_state["cfg_batch_pv_values"],
                "load_growth": float(st.session_state["cfg_batch_hosting_load_growth"]),
                "ess_size": 1.0,
                "ess_location": int(st.session_state["cfg_ess_bus_number"]),
                "control_case": st.session_state["cfg_batch_control_case"],
            }
        )
    elif mode == SCENARIO_MODE_LOAD_PV_MAP:
        settings.update(
            {
                "pv_penetration": st.session_state["cfg_batch_pv_values"],
                "load_growth": st.session_state["cfg_batch_load_values"],
                "ess_size": 1.0,
                "ess_location": int(st.session_state["cfg_ess_bus_number"]),
                "control_case": st.session_state["cfg_batch_control_case"],
            }
        )
    else:
        settings.update(
            {
                "base_pv_penetration": float(st.session_state["cfg_batch_base_pv_penetration"]),
                "base_load_growth": float(st.session_state["cfg_batch_base_load_growth"]),
                "ess_size": st.session_state["cfg_batch_ess_values"],
                "ess_location": st.session_state["cfg_batch_ess_location_values"],
                "control_case": CONTROL_CASE_OLTC_ESS,
                "base_stress_case": (
                    f"PV {float(st.session_state['cfg_batch_base_pv_penetration']):.2f}, "
                    f"Load {float(st.session_state['cfg_batch_base_load_growth']):.2f}"
                ),
            }
        )
    return settings



def render_batch_mode_panel(locked: bool) -> bool:
    """Render the existing batch expander with research-mode inputs and a lightweight preview."""
    with st.expander("Batch Scenario Runner", expanded=False):
        st.caption("Batch mode now generates research-question-driven scenarios instead of a blind Cartesian product.")
        st.selectbox(
            "Scenario Mode",
            options=BATCH_SCENARIO_MODE_OPTIONS,
            format_func=scenario_mode_label,
            key="cfg_batch_mode",
            disabled=locked,
        )
        mode = st.session_state["cfg_batch_mode"]
        mode_meta = scenario_mode_metadata(mode)
        st.caption(mode_meta["research_question"])
        st.caption(f"Fixed: {mode_meta['fixed_variables']} | Varied: {mode_meta['varied_variables']}")

        ess_baseline = (
            f"Current ESS baseline from sidebar: Bus {int(st.session_state['cfg_ess_bus_number'])}, "
            f"{float(st.session_state['cfg_ess_power_mw']):.2f} MW / {float(st.session_state['cfg_ess_capacity_mwh']):.2f} MWh"
        )

        if mode == SCENARIO_MODE_HOSTING_CAPACITY:
            st.text_input(
                "PV Penetration Sweep",
                key="cfg_batch_pv_values",
                disabled=locked,
                help="Monotonic PV sweep, e.g. 0.8,1.0,1.2,1.4 or 0.8:1.6:0.2",
            )
            st.number_input(
                "Fixed Load Growth",
                min_value=0.1,
                step=0.1,
                key="cfg_batch_hosting_load_growth",
                disabled=locked,
            )
            st.selectbox(
                "Control Case",
                options=CONTROL_CASE_OPTIONS,
                format_func=control_case_label,
                key="cfg_batch_control_case",
                disabled=locked,
            )
            st.caption(ess_baseline)
        elif mode == SCENARIO_MODE_LOAD_PV_MAP:
            st.text_input(
                "PV Penetration Grid",
                key="cfg_batch_pv_values",
                disabled=locked,
                help="Structured PV axis, e.g. 0.8,1.0,1.2,1.4",
            )
            st.text_input(
                "Load Growth Grid",
                key="cfg_batch_load_values",
                disabled=locked,
                help="Structured load axis, e.g. 0.8,1.0,1.2,1.4",
            )
            st.selectbox(
                "Control Case",
                options=CONTROL_CASE_OPTIONS,
                format_func=control_case_label,
                key="cfg_batch_control_case",
                disabled=locked,
            )
            st.caption(ess_baseline)
        else:
            st.number_input(
                "Base PV Penetration",
                min_value=0.0,
                step=0.1,
                key="cfg_batch_base_pv_penetration",
                disabled=locked,
            )
            st.number_input(
                "Base Load Growth",
                min_value=0.1,
                step=0.1,
                key="cfg_batch_base_load_growth",
                disabled=locked,
            )
            st.text_input(
                "ESS Size Sweep",
                key="cfg_batch_ess_values",
                disabled=locked,
                help="ESS size multipliers relative to the current sidebar ESS rating, e.g. 0.0,0.5,1.0,1.5",
            )
            st.text_input(
                "ESS Location Sweep",
                key="cfg_batch_ess_location_values",
                disabled=locked,
                help="Bus numbers, e.g. 3,4,5",
            )
            st.caption(ess_baseline)
            st.caption("ESS sizing fixes control case to OLTC + ESS and uses the current sidebar ESS rating as the base unit.")

        c1, c2, c3 = st.columns(3)
        with c1:
            st.checkbox("Parallel Execution", key="cfg_batch_parallel", disabled=locked)
        with c2:
            st.checkbox("Store Detailed Outputs", key="cfg_batch_include_timeseries", disabled=locked)
        with c3:
            st.number_input("Max Workers", min_value=1, step=1, key="cfg_batch_max_workers", disabled=locked)

        scenarios = []
        try:
            batch_settings = build_batch_settings_from_state()
            scenarios = generate_scenarios(mode, batch_settings)
            preview_df = build_scenario_preview_df(scenarios)
            st.caption(f"Scenario count: {len(scenarios)}")
            if not preview_df.empty:
                st.dataframe(preview_df.head(12), use_container_width=True, hide_index=True)
                if len(preview_df) > 12:
                    st.caption("Preview shows the first 12 scenarios only.")
        except Exception as exc:
            st.warning(f"Scenario specification error: {exc}")

        return st.button("Batch Scenario Execution", key="run_batch_button", disabled=locked or len(scenarios) == 0)
def render_run_controls():
    # Streamlit 동기 실행 구조에서는 페이지 내부 버튼으로 현재 루프를 즉시 끊을 수 없다.
    # 대신 실행 상태, 중단 후 복구 방법, 재실행/초기화 버튼을 항상 같은 위치에 유지한다.
    is_running = st.session_state.get("is_running", False)
    interrupted = st.session_state.get("interrupted_run_action")
    last_action = st.session_state.get("last_run_action")

    if is_running:
        st.info("시뮬레이션 실행 중입니다. 즉시 중지는 화면 우상단 Stop을 사용하고, 중단 후에는 아래 재실행 또는 초기화를 사용하세요.")
    if interrupted:
        st.warning(f"이전 시뮬레이션이 중단되었습니다: {interrupted}. 재실행 또는 초기화를 선택하세요.")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.button(
            "중지",
            key="stop_button_placeholder",
            disabled=True,
            help="현재 실행 구조에서는 페이지 내부 버튼으로 즉시 중단할 수 없습니다. 실행 중에는 우상단 Stop을 사용하세요.",
        )
    with c2:
        if st.button("재실행", key="rerun_last_action", disabled=is_running or last_action is None):
            queue_run(last_action)
    with c3:
        if st.button("초기화", key="reset_run_state", disabled=is_running):
            reset_run_state(clear_results=True)
            st.rerun()

    st.caption("`중지`: 우상단 Stop 사용, `재실행`: 마지막 실행 조건으로 다시 시작, `초기화`: 현재 결과와 실행 상태 정리")


def get_config_from_state(cncv_r: float, cncv_x: float, acsr_r: float, acsr_x: float):
    # 위젯 상태를 실제 시뮬레이션 설정 딕셔너리로 묶는다.
    return {
        "len_01": float(st.session_state["cfg_len_01"]),
        "len_12": float(st.session_state["cfg_len_12"]),
        "len_23": float(st.session_state["cfg_len_23"]),
        "len_34": float(st.session_state["cfg_len_34"]),
        "len_45": float(st.session_state["cfg_len_45"]),
        "cncv_r": float(cncv_r),
        "cncv_x": float(cncv_x),
        "acsr_r": float(acsr_r),
        "acsr_x": float(acsr_x),
        "time_step_mins": int(st.session_state["cfg_time_step_mins"]),
        "voltage_min_limit": float(st.session_state["cfg_voltage_min_limit"]),
        "voltage_max_limit": float(st.session_state["cfg_voltage_max_limit"]),
        "voltage_low_off": float(st.session_state["cfg_voltage_low_off"]),
        "voltage_high_off": float(st.session_state["cfg_voltage_high_off"]),
        "line_limit_mva": float(st.session_state["cfg_line_limit_mva"]),
        "line_return_mva": float(st.session_state["cfg_line_limit_mva"]) * float(st.session_state["cfg_line_return_ratio"]),
        "oltc_step": float(st.session_state["cfg_oltc_step"]),
        "oltc_delay_mins": float(st.session_state["cfg_oltc_delay_mins"]),
        "oltc_return_delay_mins": float(st.session_state["cfg_oltc_return_delay_mins"]),
        "ess_init_soc": float(st.session_state["cfg_ess_init_soc"]),
        "ess_efficiency": float(st.session_state["cfg_ess_efficiency"]),
        "ess_min_soc": float(st.session_state["cfg_ess_min_soc"]),
        "ess_max_soc": float(st.session_state["cfg_ess_max_soc"]),
        "ess_p_gain": float(st.session_state["cfg_ess_p_gain"]),
        "ess_q_gain": float(st.session_state["cfg_ess_q_gain"]),
        "line_relief_gain": float(st.session_state["cfg_line_relief_gain"]),
        "ess_ramp_rate_mw_per_min": float(st.session_state["cfg_ess_ramp_rate_mw_per_min"]),
        "low_load_upper_pct": float(st.session_state["cfg_low_load_upper_pct"]),
        "mid_load_upper_pct": float(st.session_state["cfg_mid_load_upper_pct"]),
        "ess_bus_number": int(st.session_state["cfg_ess_bus_number"]),
        "ess_power_mw": float(st.session_state["cfg_ess_power_mw"]),
        "ess_capacity_mwh": float(st.session_state["cfg_ess_capacity_mwh"]),
    }


def build_sidebar_config():
    locked = st.session_state.get("is_running", False)
    if locked:
        st.sidebar.warning("시뮬레이션 실행 중에는 설정 변경이 잠금됩니다.")

    st.sidebar.header("분석 시나리오")
    st.sidebar.selectbox("자동 분석 유형", options=SCENARIO_OPTIONS, format_func=scenario_label, key="cfg_scenario", disabled=locked)

    with st.sidebar.expander("선로 길이 설정 (km)", expanded=False):
        st.number_input("Line 0-1 (CNCV)", step=0.5, key="cfg_len_01", disabled=locked)
        st.number_input("Line 1-2 (ACSR)", step=0.5, key="cfg_len_12", disabled=locked)
        st.number_input("Line 2-3 (ACSR)", step=0.5, key="cfg_len_23", disabled=locked)
        st.number_input("Line 3-4 (ACSR)", step=0.5, key="cfg_len_34", disabled=locked)
        st.number_input("Line 4-5 (ACSR)", step=0.5, key="cfg_len_45", disabled=locked)

    with st.sidebar.expander("선로 임피던스 (옴/km)", expanded=False):
        st.caption("형식: R + jX")
        z_cncv_str = st.text_input("CNCV 325mm2", key="cfg_z_cncv_str", disabled=locked)
        z_acsr_str = st.text_input("ACSR 160mm2", key="cfg_z_acsr_str", disabled=locked)
        cncv_r, cncv_x = parse_impedance(z_cncv_str)
        acsr_r, acsr_x = parse_impedance(z_acsr_str)
    with st.sidebar.expander("운영 기준", expanded=True):
        st.number_input("전압 하한 (p.u.)", step=0.01, key="cfg_voltage_min_limit", disabled=locked)
        st.number_input("전압 상한 (p.u.)", step=0.01, key="cfg_voltage_max_limit", disabled=locked)
        st.number_input("선로용량 한도 (MVA)", step=0.5, key="cfg_line_limit_mva", disabled=locked)
        st.number_input("시뮬레이션 간격 (분)", min_value=1, max_value=60, step=1, key="cfg_time_step_mins", disabled=locked)

    with st.sidebar.expander("OLTC 설정", expanded=False):
        st.number_input("탭당 전압 변동률 (%)", step=0.05, key="cfg_oltc_step", disabled=locked)
        st.slider("OLTC 동작 시지연 (분)", 1, 30, key="cfg_oltc_delay_mins", disabled=locked)
        st.slider("OLTC 복귀 지연 (분)", 1, 60, key="cfg_oltc_return_delay_mins", disabled=locked)

    with st.sidebar.expander("ESS 설정", expanded=False):
        st.selectbox("ESS 설치 버스", options=BUS_OPTIONS, index=int(st.session_state["cfg_ess_bus_number"]) - 1, key="cfg_ess_bus_label", disabled=locked)
        st.number_input("ESS 정격출력 (MW)", min_value=0.0, step=0.5, key="cfg_ess_power_mw", disabled=locked)
        st.number_input("ESS 에너지용량 (MWh)", min_value=0.0, step=1.0, key="cfg_ess_capacity_mwh", disabled=locked)
        st.slider("ESS 초기 SOC (%)", 0.0, 100.0, step=5.0, key="cfg_ess_init_soc", disabled=locked)
        st.number_input("ESS 효율 (0~1)", min_value=0.01, max_value=1.0, step=0.01, key="cfg_ess_efficiency", disabled=locked)
        st.number_input("ESS 최소 SOC (%)", min_value=0.0, max_value=100.0, step=1.0, key="cfg_ess_min_soc", disabled=locked)
        st.number_input("ESS 최대 SOC (%)", min_value=0.0, max_value=100.0, step=1.0, key="cfg_ess_max_soc", disabled=locked)

    with st.sidebar.expander("부하구간 기준", expanded=False):
        st.number_input("경부하 상한 (%)", min_value=0.0, max_value=100.0, step=5.0, key="cfg_low_load_upper_pct", disabled=locked)
        st.number_input("중간부하 상한 (%)", min_value=0.0, max_value=100.0, step=5.0, key="cfg_mid_load_upper_pct", disabled=locked)

    with st.sidebar.expander("자동 분석 범위", expanded=False):
        st.number_input("증가 시작 배율", min_value=0.1, step=0.1, key="cfg_auto_start_scale", disabled=locked)
        st.number_input("증가 배율 간격", min_value=0.01, step=0.01, key="cfg_auto_step", disabled=locked)
        st.number_input("최대 증가 배율", min_value=0.2, step=0.1, key="cfg_auto_max_scale", disabled=locked)

    st.session_state["cfg_ess_bus_number"] = BUS_INDEX_MAP[st.session_state["cfg_ess_bus_label"]]
    return get_config_from_state(cncv_r, cncv_x, acsr_r, acsr_x)


def editable_bus_dataframe(bus_df: pd.DataFrame) -> pd.DataFrame:
    return bus_df[[BUS_COL, LOAD_P_COL, LOAD_Q_COL, PV_P_COL, WIND_P_COL]].copy()


def merge_bus_editor(edited_df: pd.DataFrame):
    # 사용자가 수정하는 버스 데이터는 부하/PV/Wind만 반영하고 ESS는 별도 설정으로 관리한다.
    base = st.session_state["bus_df"].copy()
    for col in [BUS_COL, LOAD_P_COL, LOAD_Q_COL, PV_P_COL, WIND_P_COL]:
        base[col] = edited_df[col]
    st.session_state["bus_df"] = base


def apply_recommended_base_case():
    # 재생에너지 증가 시 역송전이 과도해지지 않도록 기본 부하를 총 15 MW 수준으로 조정한다.
    bus_df = st.session_state["bus_df"].copy()
    bus_df[LOAD_P_COL] = [2.4, 2.7, 3.0, 3.3, 3.6]
    bus_df[LOAD_Q_COL] = [0.24, 0.27, 0.30, 0.33, 0.36]
    bus_df[PV_P_COL] = [0.0, 0.0, 2.0, 4.0, 8.0]
    bus_df[WIND_P_COL] = [0.0, 5.0, 0.0, 0.0, 0.0]
    st.session_state["bus_df"] = bus_df
    st.session_state["cfg_ess_bus_number"] = 5
    st.session_state["cfg_ess_bus_label"] = "Bus 5"
    st.session_state["cfg_ess_power_mw"] = 5.0
    st.session_state["cfg_ess_capacity_mwh"] = 15.0


def render_topology(display_bus_df: pd.DataFrame):
    def get_components(idx: int):
        comps = []
        if float(display_bus_df.at[idx, LOAD_P_COL]) > 0:
            comps.append("Load")
        if float(display_bus_df.at[idx, PV_P_COL]) > 0:
            comps.append("PV")
        if float(display_bus_df.at[idx, WIND_P_COL]) > 0:
            comps.append("Wind")
        if idx + 1 == int(st.session_state["cfg_ess_bus_number"]) and float(st.session_state["cfg_ess_power_mw"]) > 0:
            comps.append("ESS")
        return "<br>".join(comps) if comps else "빈 모선"

    diagram_html = f"""
<div style="display:flex;justify-content:space-between;align-items:center;background:#f2f5f8;padding:18px;border-radius:12px;color:#111;text-align:center;font-size:13px;">
    <div><b>Substation</b><br>OLTC</div>
    <div>▶<br><span style="font-size:11px;color:#466;">{st.session_state['cfg_len_01']}km</span></div>
    <div><b>Bus 1</b><br><span style="font-size:12px;">{get_components(0)}</span></div>
    <div>▶<br><span style="font-size:11px;color:#466;">{st.session_state['cfg_len_12']}km</span></div>
    <div><b>Bus 2</b><br><span style="font-size:12px;">{get_components(1)}</span></div>
    <div>▶<br><span style="font-size:11px;color:#466;">{st.session_state['cfg_len_23']}km</span></div>
    <div><b>Bus 3</b><br><span style="font-size:12px;">{get_components(2)}</span></div>
    <div>▶<br><span style="font-size:11px;color:#466;">{st.session_state['cfg_len_34']}km</span></div>
    <div><b>Bus 4</b><br><span style="font-size:12px;">{get_components(3)}</span></div>
    <div>▶<br><span style="font-size:11px;color:#466;">{st.session_state['cfg_len_45']}km</span></div>
    <div><b>Bus 5</b><br><span style="font-size:12px;">{get_components(4)}</span></div>
</div>
"""
    st.markdown(diagram_html, unsafe_allow_html=True)


def load_uploaded_profile(disabled: bool = False):
    uploaded_file = st.file_uploader("시간 패턴 파일 업로드 (CSV/XLSX)", type=["xlsx", "csv"], key="profile_uploader", disabled=disabled)
    if uploaded_file is None:
        return
    signature = f"{uploaded_file.name}:{uploaded_file.size}"
    if signature == st.session_state["uploaded_profile_signature"]:
        return
    try:
        uploaded_df = pd.read_csv(uploaded_file) if uploaded_file.name.lower().endswith(".csv") else pd.read_excel(uploaded_file)
        st.session_state["time_df"] = uploaded_df
        st.session_state["uploaded_profile_signature"] = signature
        st.success("시간 패턴 파일을 반영했습니다.")
    except Exception as exc:
        st.error(f"파일 로드 오류: {exc}")


def render_algorithm_tab():
    # 알고리즘 탭은 preset 적용과 세부 파라미터 수정을 한 곳에서 수행한다.
    locked = st.session_state.get("is_running", False)
    st.subheader("협조제어 알고리즘 수정")
    if locked:
        st.warning("시뮬레이션 실행 중에는 알고리즘 파라미터를 변경할 수 없습니다.")
        return

    st.markdown("### 알고리즘 preset")
    p1, p2 = st.columns(2)
    with p1:
        if st.button("현재 기본 알고리즘 복원", key="algo_preset_baseline"):
            queue_algorithm_preset("baseline")
    with p2:
        if st.button("첨부 문서 권장값 적용", key="algo_preset_report"):
            queue_algorithm_preset("report_recommended")

    with st.expander("현재 구현된 OLTC-ESS 운영 방식", expanded=True):
        st.markdown(
            """
- 상태기반 제어를 사용합니다: `NORMAL`, `UNDERVOLTAGE`, `OVERVOLTAGE`, `CONGESTION`
- 우선순위는 `선로 혼잡 > 전압 이상`입니다.
- `저전압`이면 OLTC 탭 조정을 먼저 시도하고, ESS가 방전(+P)과 무효전력 주입(+Q)으로 보조합니다.
- `과전압`이면 OLTC 탭 조정을 먼저 시도하고, ESS가 충전(-P)과 무효전력 흡수(-Q)로 보조합니다.
- `선로 혼잡`이면 ESS가 먼저 조류 완화 방향으로 동작하고, 전압 이상이 함께 있을 때만 OLTC가 보조적으로 개입합니다.
- `정상 상태`에서는 ESS 출력은 ramp-to-zero로 천천히 0으로 복귀하고, OLTC는 복귀 지연시간 이후 0 tap 근처로 단계 복귀합니다.
- ESS는 `SOC 최소/최대`, `출력 한계`, `효율` 제약을 함께 적용합니다.
            """
        )

    with st.expander("첨부 문서의 협조제어 알고리즘 요약", expanded=True):
        st.markdown(
            """
- 문서는 `OLTC를 느린 1차 제어기`, `ESS를 빠른 2차 보조 제어기`로 둡니다.
- 목적은 `전압 유지`, `선로용량 초과 방지`, `OLTC 동작 최소화`, `ESS 사용 최소화`입니다.
- OLTC는 `±8 tap`, `1 tap당 1.25%`, `dead-band + waiting time`을 갖는 이산 제어기입니다.
- ESS는 `유효전력 + 무효전력`을 함께 제어하고, `SOC lock`, `피상전력 한계`, `복귀 램프`를 포함합니다.
- 상태는 `정상`, `저전압/중부하`, `과전압/PV 과다`, `선로 혼잡`, `회복/복귀`로 구분됩니다.
- 핵심 우선순위는 `선로 과부하 우선`, 그리고 `전압 문제는 OLTC 우선 + ESS 보조`입니다.
- 정상 복귀 시에는 ESS를 즉시 0으로 만들지 말고 천천히 복귀시키고, OLTC도 안정 조건에서 0 tap 근처로 단계 복귀시킵니다.
            """
        )

    with st.form("algo_form"):
        col1, col2 = st.columns(2)
        with col1:
            algo_voltage_min = st.number_input("전압 하한 (p.u.)", value=float(st.session_state["cfg_voltage_min_limit"]), step=0.01, key="algo_voltage_min")
            algo_voltage_low_off = st.number_input("저전압 해제 기준 (p.u.)", value=float(st.session_state["cfg_voltage_low_off"]), step=0.005, key="algo_voltage_low_off")
            algo_line_limit = st.number_input("선로용량 한도 (MVA)", value=float(st.session_state["cfg_line_limit_mva"]), step=0.5, key="algo_line_limit")
            algo_line_return_ratio = st.number_input("선로 복귀 비율 (0~1)", value=float(st.session_state["cfg_line_return_ratio"]), min_value=0.50, max_value=1.0, step=0.01, key="algo_line_return_ratio")
            algo_oltc_delay = st.number_input("OLTC 동작 지연 (분)", value=float(st.session_state["cfg_oltc_delay_mins"]), step=1.0, key="algo_oltc_delay")
            algo_oltc_return = st.number_input("OLTC 복귀 지연 (분)", value=float(st.session_state["cfg_oltc_return_delay_mins"]), step=1.0, key="algo_oltc_return")
            algo_ess_p = st.number_input("ESS 유효전력 이득", value=float(st.session_state["cfg_ess_p_gain"]), step=0.5, key="algo_ess_p")
            algo_ess_min_soc = st.number_input("ESS 최소 SOC lock (%)", value=float(st.session_state["cfg_ess_min_soc"]), step=1.0, key="algo_ess_min_soc")
        with col2:
            algo_voltage_max = st.number_input("전압 상한 (p.u.)", value=float(st.session_state["cfg_voltage_max_limit"]), step=0.01, key="algo_voltage_max")
            algo_voltage_high_off = st.number_input("과전압 해제 기준 (p.u.)", value=float(st.session_state["cfg_voltage_high_off"]), step=0.005, key="algo_voltage_high_off")
            algo_line_relief = st.number_input("선로 혼잡 완화 이득", value=float(st.session_state["cfg_line_relief_gain"]), step=0.5, key="algo_line_relief")
            algo_ess_q = st.number_input("ESS 무효전력 이득", value=float(st.session_state["cfg_ess_q_gain"]), step=0.5, key="algo_ess_q")
            algo_ramp = st.number_input("ESS 램프율 (MW/분)", value=float(st.session_state["cfg_ess_ramp_rate_mw_per_min"]), step=0.05, key="algo_ramp")
            algo_step = st.number_input("OLTC 탭당 전압 변동률 (%)", value=float(st.session_state["cfg_oltc_step"]), step=0.05, key="algo_step")
            algo_ess_max_soc = st.number_input("ESS 최대 SOC lock (%)", value=float(st.session_state["cfg_ess_max_soc"]), step=1.0, key="algo_ess_max_soc")
        submitted = st.form_submit_button("알고리즘 설정 적용", type="primary")

    if submitted:
        st.session_state["pending_cfg_update"] = {
            "cfg_voltage_min_limit": float(algo_voltage_min),
            "cfg_voltage_max_limit": float(algo_voltage_max),
            "cfg_voltage_low_off": float(algo_voltage_low_off),
            "cfg_voltage_high_off": float(algo_voltage_high_off),
            "cfg_line_limit_mva": float(algo_line_limit),
            "cfg_line_return_ratio": float(algo_line_return_ratio),
            "cfg_oltc_delay_mins": float(algo_oltc_delay),
            "cfg_oltc_return_delay_mins": float(algo_oltc_return),
            "cfg_ess_p_gain": float(algo_ess_p),
            "cfg_ess_q_gain": float(algo_ess_q),
            "cfg_line_relief_gain": float(algo_line_relief),
            "cfg_ess_ramp_rate_mw_per_min": float(algo_ramp),
            "cfg_oltc_step": float(algo_step),
            "cfg_ess_min_soc": float(algo_ess_min_soc),
            "cfg_ess_max_soc": float(algo_ess_max_soc),
        }
        st.rerun()


def active_ess_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    cols = [col for col in df.columns if df[col].notna().any() and float(df[col].abs().fillna(0.0).sum()) > 0.0]
    return df[cols] if cols else df.iloc[:, :0]


def display_results():
    results = st.session_state.get("sim_results")
    events = st.session_state.get("sim_events")
    if results is None or events is None:
        return

    context = st.session_state.get("sim_result_context") or {"label": "단일 기준 시나리오 (load=100%, renewable=100%)"}
    st.header("2. 상세 시뮬레이션 결과")
    verdict = "적합" if events.get("overall_ok") else "부적합"
    st.caption(
        f"표시 대상: {context.get('label', '-')} | 전압 {events.get('global_min_voltage', float('nan')):.4f} ~ {events.get('global_max_voltage', float('nan')):.4f} p.u. | "
        f"최대 선로용량 {events.get('global_max_line_mva', float('nan')):.3f} MVA | OLTC 동작 {events.get('oltc_moves', 0)}회 | "
        f"ESS 위치 Bus {events.get('ess_bus_number', '-')} | 판정 {verdict}"
    )

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("##### 모선 전압 프로파일")
        fig_v = px.line(results["df_v"], labels={"index": "시간", "value": "Voltage (p.u.)", "variable": "모선"})
        fig_v.add_hline(y=float(st.session_state["cfg_voltage_min_limit"]), line_dash="dash", line_color="red")
        fig_v.add_hline(y=float(st.session_state["cfg_voltage_max_limit"]), line_dash="dash", line_color="red")
        fig_v.update_layout(margin=dict(l=0, r=0, t=20, b=0), hovermode="x unified")
        st.plotly_chart(fig_v, use_container_width=True)

        st.markdown("##### 최대 선로용량 추이")
        fig_line = px.line(results["df_line_mva_max"], labels={"index": "시간", "value": "MVA"})
        fig_line.add_hline(y=float(st.session_state["cfg_line_limit_mva"]), line_dash="dash", line_color="red")
        fig_line.update_layout(margin=dict(l=0, r=0, t=20, b=0), hovermode="x unified")
        st.plotly_chart(fig_line, use_container_width=True)

    with col2:
        st.markdown("##### ESS SOC")
        df_soc = active_ess_frame(results["df_soc"])
        if df_soc.empty:
            st.info("활성 ESS가 없습니다.")
        else:
            fig_soc = px.line(df_soc, labels={"index": "시간", "value": "SOC (%)", "variable": "ESS 위치"})
            fig_soc.update_layout(margin=dict(l=0, r=0, t=20, b=0), hovermode="x unified")
            st.plotly_chart(fig_soc, use_container_width=True)

        st.markdown("##### ESS 유효전력 출력")
        df_ess = active_ess_frame(results["df_ess_p"])
        if df_ess.empty:
            st.info("활성 ESS가 없습니다.")
        else:
            fig_ess = px.line(df_ess, labels={"index": "시간", "value": "MW", "variable": "ESS 위치"})
            fig_ess.update_layout(margin=dict(l=0, r=0, t=20, b=0), hovermode="x unified")
            st.plotly_chart(fig_ess, use_container_width=True)

    st.markdown("##### 상태 및 판정")
    summary_df = pd.concat([results["df_min_v"], results["df_max_v"], results["df_line_mva_max"], results["df_state"]], axis=1)
    st.dataframe(summary_df, use_container_width=True)
    st.download_button(
        label="상세 결과 엑셀 다운로드",
        data=st.session_state["excel_bytes"],
        file_name="simulation_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="download_excel_button",
    )


def display_auto_results():
    search_result = st.session_state.get("auto_search_result")
    if not search_result:
        return

    st.header("3. 자동 민감도 분석 결과")
    runs_df = pd.DataFrame(search_result["runs"])
    scenario = search_result.get("scenario")
    if runs_df.empty:
        return

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        if scenario == SCENARIO_RENEWABLE_BY_LOAD_LEVEL:
            plot_df = runs_df[["sweep_percent", "경부하_min_voltage", "중간부하_min_voltage", "중부하_min_voltage"]].melt(
                id_vars="sweep_percent", var_name="구분", value_name="최소전압"
            )
            fig = px.line(plot_df, x="sweep_percent", y="최소전압", color="구분", markers=True)
            fig.update_layout(title="재생에너지 증가에 따른 부하구간별 최소전압 변화", xaxis_title="증가 배율 (%)", yaxis_title="Voltage (p.u.)")
        else:
            plot_df = runs_df[["sweep_percent", "min_voltage", "max_voltage"]].melt(id_vars="sweep_percent", var_name="항목", value_name="전압")
            fig = px.line(plot_df, x="sweep_percent", y="전압", color="항목", markers=True)
            fig.update_layout(title="증가 배율에 따른 전압 변화", xaxis_title="증가 배율 (%)", yaxis_title="Voltage (p.u.)")
        fig.add_hline(y=float(st.session_state["cfg_voltage_min_limit"]), line_dash="dash", line_color="red")
        fig.add_hline(y=float(st.session_state["cfg_voltage_max_limit"]), line_dash="dash", line_color="red")
        st.plotly_chart(fig, use_container_width=True)

    with chart_col2:
        fig_line = px.line(runs_df, x="sweep_percent", y="max_line_mva", markers=True, labels={"sweep_percent": "증가 배율 (%)", "max_line_mva": "Max Line MVA"})
        fig_line.add_hline(y=float(st.session_state["cfg_line_limit_mva"]), line_dash="dash", line_color="red")
        fig_line.update_layout(title="증가 배율에 따른 선로용량 변화")
        st.plotly_chart(fig_line, use_container_width=True)

    display_cols = [
        "sweep_percent", "load_scale", "renewable_scale", "min_voltage", "max_voltage", "max_line_mva",
        "load_total_min_mw", "load_total_max_mw", "pv_total_min_mw", "pv_total_max_mw", "wind_total_min_mw", "wind_total_max_mw",
        "ess_power_min_mw", "ess_power_max_mw", "ess_soc_min_pct", "ess_soc_max_pct", "oltc_tap_min", "oltc_tap_max",
        "min_voltage_sensitivity", "voltage_ok", "line_ok", "overall_ok", "oltc_moves", "final_tap"
    ]
    extra_cols = [c for c in ["경부하_min_voltage", "중간부하_min_voltage", "중부하_min_voltage"] if c in runs_df.columns]
    st.dataframe(runs_df[display_cols + extra_cols], use_container_width=True)

    first_failure = search_result.get("first_failure")
    if first_failure:
        st.error(
            f"최초 부적합 단계: {first_failure['sweep_percent']:.1f}% | 최소전압 {first_failure['min_voltage']:.4f} p.u. | "
            f"최대전압 {first_failure['max_voltage']:.4f} p.u. | 최대선로 {first_failure['max_line_mva']:.3f} MVA"
        )
    else:
        st.success("설정한 증가 범위 내에서는 전압 및 선로용량 기준을 모두 만족했습니다.")

    if st.session_state.get("auto_report_bytes"):
        st.download_button(
            label="분석 보고서 다운로드 (.docx)",
            data=st.session_state["auto_report_bytes"],
            file_name=st.session_state.get("auto_report_name", "simulation_report.docx"),
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            key="download_word_report",
        )



def display_batch_results():
    batch_result = st.session_state.get("batch_result")
    if not batch_result:
        return

    st.header("4. Batch Scenario Summary")
    summary_df = batch_result.get("summary_df", pd.DataFrame())
    aggregate_df = batch_result.get("aggregate_df", pd.DataFrame())
    st.caption(
        f"Mode: {batch_result.get('execution_mode', 'serial')} | Workers: {batch_result.get('max_workers', 1)} | "
        f"Elapsed: {float(batch_result.get('elapsed_sec', 0.0)):.2f}s"
    )
    if batch_result.get("fallback_reason"):
        st.info(f"Fallback / note: {batch_result['fallback_reason']}")

    if isinstance(aggregate_df, pd.DataFrame) and not aggregate_df.empty:
        st.markdown("##### Aggregate Summary")
        st.dataframe(aggregate_df, use_container_width=True)

    if isinstance(summary_df, pd.DataFrame) and not summary_df.empty:
        st.markdown("##### Scenario Summary")
        st.dataframe(summary_df, use_container_width=True)

    d1, d2 = st.columns(2)
    with d1:
        if st.session_state.get("batch_summary_csv_bytes"):
            st.download_button(
                label="Batch Summary CSV",
                data=st.session_state["batch_summary_csv_bytes"],
                file_name=f"{st.session_state.get('batch_summary_prefix', 'batch_summary')}.csv",
                mime="text/csv",
                key="download_batch_csv",
            )
    with d2:
        if st.session_state.get("batch_summary_excel_bytes"):
            st.download_button(
                label="Batch Summary Excel",
                data=st.session_state["batch_summary_excel_bytes"],
                file_name=f"{st.session_state.get('batch_summary_prefix', 'batch_summary')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_batch_excel",
            )

def run_single_scenario(config):
    # 현재 설정값으로 24시간 단일 시나리오를 1회 수행한다.
    progress = st.progress(0, text="단일 시나리오 계산 중...")
    try:
        def _progress_cb(minute, total):
            progress.progress(min(minute / max(total, 1), 1.0), text=f"조류해석 수행 중... ({minute}/{total}분)")

        single_output = run_single_simulation(
            config=config,
            bus_df=st.session_state["bus_df"],
            time_df=st.session_state["time_df"],
            include_timeseries=True,
            ess_efficiency=config["ess_efficiency"],
            time_step_mins=config["time_step_mins"],
            progress_cb=_progress_cb,
        )
        results = single_output["results"]
        events = single_output["events"]
        st.session_state["sim_results"] = results
        st.session_state["sim_events"] = events
        st.session_state["sim_result_context"] = {"label": "단일 기준 시나리오 (load=100%, renewable=100%)"}
        st.session_state["excel_bytes"] = build_analysis_excel_bytes(results)
        progress.progress(1.0, text="단일 시나리오 완료")
    finally:
        st.session_state["is_running"] = False
        st.session_state["active_run_action"] = None


def run_auto_scenario(config):
    # 자동 민감도 분석은 시작 배율부터 최대 배율까지 단계별로 반복 시뮬레이션을 수행한다.
    start_scale = float(st.session_state["cfg_auto_start_scale"])
    step_scale = float(st.session_state["cfg_auto_step"])
    max_scale = float(st.session_state["cfg_auto_max_scale"])
    scenario = st.session_state["cfg_scenario"]

    if max_scale < start_scale:
        st.error("최대 증가 배율은 시작 배율보다 크거나 같아야 합니다.")
        return
    if step_scale <= 0:
        st.error("증가 간격은 0보다 커야 합니다.")
        return

    progress = st.progress(0, text="자동 민감도 분석 초기화 중...")
    try:
        estimated_runs = max(1, int(math.floor((max_scale - start_scale) / step_scale)) + 1)

        def _auto_progress(minute, total, current_scale):
            run_idx = int(round((current_scale - start_scale) / step_scale)) if step_scale > 0 else 0
            run_idx = max(0, min(run_idx, estimated_runs - 1))
            ratio = (run_idx + (minute / max(total, 1))) / estimated_runs
            progress.progress(min(ratio, 0.995), text=f"{scenario_label(scenario)}: {current_scale * 100:.1f}%")

        search_result = run_sensitivity_search(
            config=config,
            bus_df=st.session_state["bus_df"],
            time_df=st.session_state["time_df"],
            scenario=scenario,
            start_scale=start_scale,
            step=step_scale,
            max_scale=max_scale,
            ess_efficiency=config["ess_efficiency"],
            time_step_mins=config["time_step_mins"],
            total_minutes=24 * 60,
            progress_cb=_auto_progress,
        )
        st.session_state["auto_search_result"] = search_result
        st.session_state["auto_report_bytes"] = build_word_report_bytes(search_result)
        st.session_state["auto_report_name"] = f"simulation_report_{scenario}.docx"

        if search_result.get("first_failure_results") is not None:
            st.session_state["sim_results"] = search_result["first_failure_results"]
            st.session_state["sim_events"] = search_result["first_failure_events"]
            st.session_state["sim_result_context"] = {
                "label": f"{scenario_label(scenario)} | 최초 부적합 단계 {float(search_result['first_failure']['sweep_percent']):.1f}%"
            }
            st.session_state["excel_bytes"] = build_analysis_excel_bytes(search_result["first_failure_results"])
        elif search_result.get("last_results") is not None:
            st.session_state["sim_results"] = search_result["last_results"]
            st.session_state["sim_events"] = search_result["last_events"]
            st.session_state["sim_result_context"] = {"label": f"{scenario_label(scenario)} | 최종 단계 {float(search_result['max_scale']) * 100:.1f}%"}
            st.session_state["excel_bytes"] = build_analysis_excel_bytes(search_result["last_results"])
        progress.progress(1.0, text="자동 민감도 분석 완료")
    finally:
        st.session_state["is_running"] = False
        st.session_state["active_run_action"] = None



def run_batch_scenarios(config):
    # Keep the existing UI flow and execute only the generated research scenarios.
    batch_settings = build_batch_settings_from_state()
    batch_mode = st.session_state["cfg_batch_mode"]
    try:
        scenarios = generate_scenarios(batch_mode, batch_settings)
    except Exception as exc:
        st.error(f"배치 시나리오 생성 오류: {exc}")
        st.session_state["is_running"] = False
        st.session_state["active_run_action"] = None
        return

    if not scenarios:
        st.error("실행할 배치 시나리오가 없습니다.")
        st.session_state["is_running"] = False
        st.session_state["active_run_action"] = None
        return

    st.session_state["batch_result"] = None
    st.session_state["batch_summary_csv_bytes"] = None
    st.session_state["batch_summary_excel_bytes"] = None
    progress = st.progress(0, text="Batch scenario execution: preparing...")
    try:
        def _batch_progress(completed, total, scenario_id):
            progress.progress(min(completed / max(total, 1), 1.0), text=f"Batch scenario execution: {completed}/{total} ({scenario_id})")

        batch_result = run_batch_simulations(
            scenarios=scenarios,
            config=config,
            bus_df=st.session_state["bus_df"],
            time_df=st.session_state["time_df"],
            ess_efficiency=config["ess_efficiency"],
            time_step_mins=config["time_step_mins"],
            total_minutes=24 * 60,
            max_workers=int(st.session_state["cfg_batch_max_workers"]),
            parallel=bool(st.session_state["cfg_batch_parallel"]),
            include_timeseries=bool(st.session_state["cfg_batch_include_timeseries"]),
            progress_cb=_batch_progress,
        )
        st.session_state["batch_result"] = batch_result
        st.session_state["batch_summary_csv_bytes"] = build_batch_summary_csv_bytes(batch_result)
        st.session_state["batch_summary_excel_bytes"] = build_batch_summary_excel_bytes(batch_result)
        st.session_state["batch_summary_prefix"] = f"batch_{batch_mode}_summary"
        progress.progress(1.0, text=f"{scenario_mode_label(batch_mode)} batch complete")
    finally:
        st.session_state["is_running"] = False
        st.session_state["active_run_action"] = None

def main():
    init_session_state()
    apply_pending_config_updates()
    recover_interrupted_run_state()
    config = build_sidebar_config()
    locked = st.session_state.get("is_running", False)

    render_run_controls()
    display_bus_df = prepare_single_ess_bus_df(st.session_state["bus_df"], config)

    tab_model, tab_profile, tab_algo = st.tabs(["계통 모델", "시계열 패턴", "협조제어 알고리즘"])

    with tab_model:
        st.subheader("기본 계통 모델")
        st.caption("OLTC는 변전소 1대, ESS는 선택한 버스에 1대만 배치됩니다.")
        if st.button("권장 기본값 적용", key="apply_recommended_case", disabled=locked):
            apply_recommended_base_case()
        edited_df_bus = st.data_editor(editable_bus_dataframe(st.session_state["bus_df"]), hide_index=True, width="stretch", key="bus_editor", disabled=locked)
        merge_bus_editor(edited_df_bus)
        display_bus_df = prepare_single_ess_bus_df(st.session_state["bus_df"], config)
        ess_row = display_bus_df.iloc[int(st.session_state["cfg_ess_bus_number"]) - 1]
        st.write(f"ESS 설치 위치: Bus {int(st.session_state['cfg_ess_bus_number'])} | 정격출력 {float(ess_row['ESS_최대출력']):.2f} MW | 에너지용량 {float(ess_row['ESS_용량']):.2f} MWh")
        render_topology(display_bus_df)

    with tab_profile:
        st.subheader("24시간 시계열 패턴")
        if locked:
            st.info("시뮬레이션 실행 중에는 시간 패턴을 수정할 수 없습니다.")
        load_uploaded_profile(disabled=locked)
        edited_df_time = st.data_editor(st.session_state["time_df"], num_rows="dynamic", hide_index=True, width="stretch", key="time_editor", disabled=locked)
        st.session_state["time_df"] = edited_df_time.copy()

    with tab_algo:
        render_algorithm_tab()

    st.caption("자동 민감도 분석 실행은 시작 배율부터 최대 배율까지 단계별로 다수의 시뮬레이션을 반복 수행합니다.")
    b1, b2 = st.columns(2)
    with b1:
        run_single = st.button("단일 시나리오 실행", type="primary", key="run_button", disabled=locked)
    with b2:
        run_auto = st.button("자동 민감도 분석 실행", type="primary", key="run_auto_button", disabled=locked)

    run_batch = render_batch_mode_panel(locked)

    if run_single and not locked:
        queue_run("single")
    if run_auto and not locked:
        queue_run("auto")
    if run_batch and not locked:
        queue_run("batch")

    requested_action = st.session_state.get("run_request")
    if requested_action and not st.session_state.get("is_running", False):
        st.session_state["active_run_action"] = requested_action
        st.session_state["run_request"] = None
        st.session_state["is_running"] = True
        if requested_action == "single":
            st.info("시뮬레이션 실행 중입니다. 실행이 완료될 때까지 설정 변경은 잠금됩니다.")
            run_single_scenario(config)
        elif requested_action == "auto":
            st.info("자동 민감도 분석 실행 중입니다. 실행이 완료될 때까지 설정 변경은 잠금됩니다.")
            run_auto_scenario(config)
        elif requested_action == "batch":
            st.info("배치 시나리오 실행 중입니다. 실행이 완료될 때까지 설정 변경은 잠금됩니다.")
            run_batch_scenarios(config)

    display_results()
    display_auto_results()
    display_batch_results()


main()

if __name__ == "__main__":
    from streamlit import runtime

    if not runtime.exists():
        subprocess.run([sys.executable, "-m", "streamlit", "run", sys.argv[0]])






