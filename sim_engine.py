from __future__ import annotations

import io
import math
from typing import Any, Callable, Dict, Optional, Tuple

import numpy as np
import pandapower as pp
import pandas as pd

try:
    import numba  # noqa: F401

    HAS_NUMBA = True
except ImportError:
    HAS_NUMBA = False


TIME_COL = "시간 (Hour)"
MINUTE_COL = "분 (Minute)"
LOAD_PATTERN_COL = "부하 패턴 (%)"
PV_PATTERN_COL = "태양광 패턴 (%)"
WIND_PATTERN_COL = "풍력 패턴 (%)"

BUS_COL = "모선"
LOAD_P_COL = "Load_P"
LOAD_Q_COL = "Load_Q"
PV_P_COL = "PV_P"
WIND_P_COL = "Wind_P"
ESS_MAX_COL = "ESS_최대출력"
ESS_CAP_COL = "ESS_용량"


def default_config() -> Dict[str, float]:
    return {
        "len_01": 3.0,
        "len_12": 1.0,
        "len_23": 1.0,
        "len_34": 1.0,
        "len_45": 1.0,
        "cncv_r": 0.07,
        "cncv_x": 0.12,
        "acsr_r": 0.18,
        "acsr_x": 0.39,
        "oltc_step": 1.25,
        "oltc_delay_mins": 3,
        "v_upper_limit": 1.05,
        "v_lower_limit": 0.95,
        "ess_init_soc": 20.0,
        "ess_target_v": 1.04,
        "ess_discharge_v": 0.96,
        "ess_efficiency": 0.95,
        "time_step_mins": 10,
    }


def default_bus_dataframe(bus_count: int = 5) -> pd.DataFrame:
    # 기본 케이스는 재생에너지 총량 대비 부하가 너무 낮지 않도록 총 15 MW 수준으로 설정한다.
    size = max(5, bus_count)
    df = pd.DataFrame(
        {
            BUS_COL: [f"Bus {i}" for i in range(1, size + 1)],
            LOAD_P_COL: [2.4, 2.7, 3.0, 3.3, 3.6] + [3.6] * (size - 5),
            LOAD_Q_COL: [0.24, 0.27, 0.30, 0.33, 0.36] + [0.36] * (size - 5),
            PV_P_COL: [0.0, 0.0, 2.0, 4.0, 8.0] + [8.0] * (size - 5),
            WIND_P_COL: [0.0, 5.0, 0.0, 0.0, 0.0] + [0.0] * (size - 5),
            ESS_MAX_COL: [0.0, 0.0, 0.0, 0.0, 5.0] + [0.0] * (size - 5),
            ESS_CAP_COL: [0.0, 0.0, 0.0, 0.0, 15.0] + [0.0] * (size - 5),
        }
    )
    return df.iloc[:bus_count].copy()


def default_time_profile_dataframe() -> pd.DataFrame:
    hours = list(range(25))
    load_pattern = [
        40,
        38,
        35,
        35,
        40,
        45,
        60,
        75,
        85,
        90,
        95,
        100,
        95,
        90,
        85,
        80,
        75,
        80,
        90,
        95,
        85,
        70,
        55,
        45,
        40,
    ]
    pv_pattern = [0] * 6 + [5, 20, 50, 70, 90, 100, 95, 80, 50, 20, 5] + [0] * 8
    wind_pattern = [
        30,
        35,
        40,
        45,
        40,
        35,
        30,
        30,
        25,
        20,
        20,
        25,
        30,
        35,
        40,
        50,
        60,
        70,
        65,
        55,
        45,
        40,
        35,
        30,
        30,
    ]
    return pd.DataFrame(
        {
            TIME_COL: hours,
            LOAD_PATTERN_COL: load_pattern,
            PV_PATTERN_COL: pv_pattern,
            WIND_PATTERN_COL: wind_pattern,
        }
    )


def load_time_profile(file_path: str) -> pd.DataFrame:
    if file_path.lower().endswith(".csv"):
        return pd.read_csv(file_path)
    return pd.read_excel(file_path)


def format_minute(minute: int) -> str:
    return f"{minute // 60:02d}:{minute % 60:02d}"


def _build_time_grid(total_minutes: int, time_step_mins: int) -> list[int]:
    if time_step_mins <= 0:
        raise ValueError("time_step_mins must be > 0")
    grid = list(range(0, total_minutes + 1, time_step_mins))
    if not grid:
        return [0, total_minutes]
    if grid[-1] != total_minutes:
        grid.append(total_minutes)
    return sorted(set(grid))


def _normalize_bus_dataframe(bus_df: pd.DataFrame) -> pd.DataFrame:
    required_cols = [
        BUS_COL,
        LOAD_P_COL,
        LOAD_Q_COL,
        PV_P_COL,
        WIND_P_COL,
        ESS_MAX_COL,
        ESS_CAP_COL,
    ]
    normalized = bus_df.copy()
    for col in required_cols:
        if col not in normalized.columns:
            if col == BUS_COL:
                normalized[col] = [f"Bus {i}" for i in range(1, len(normalized) + 1)]
            else:
                normalized[col] = 0.0

    normalized = normalized[required_cols].copy()
    normalized = normalized.reset_index(drop=True)

    if len(normalized) == 0:
        normalized = default_bus_dataframe()

    for col in [LOAD_P_COL, LOAD_Q_COL, PV_P_COL, WIND_P_COL, ESS_MAX_COL, ESS_CAP_COL]:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce").fillna(0.0)
    return normalized


def prepare_time_profile(
    time_df: pd.DataFrame,
    total_minutes: int = 24 * 60,
    time_step_mins: int = 1,
) -> pd.DataFrame:
    working = time_df.copy()
    if TIME_COL in working.columns:
        working[MINUTE_COL] = pd.to_numeric(working[TIME_COL], errors="coerce").fillna(0.0) * 60.0
    else:
        working[MINUTE_COL] = np.arange(len(working)) * 60.0

    for col in [LOAD_PATTERN_COL, PV_PATTERN_COL, WIND_PATTERN_COL]:
        if col not in working.columns:
            working[col] = 0.0
        working[col] = pd.to_numeric(working[col], errors="coerce")

    working[MINUTE_COL] = pd.to_numeric(working[MINUTE_COL], errors="coerce").fillna(0.0).astype(int)
    working = working.sort_values(MINUTE_COL).drop_duplicates(subset=[MINUTE_COL], keep="last")

    minute_grid = pd.DataFrame({MINUTE_COL: _build_time_grid(total_minutes, time_step_mins)})
    merged = minute_grid.merge(
        working[[MINUTE_COL, LOAD_PATTERN_COL, PV_PATTERN_COL, WIND_PATTERN_COL]],
        on=MINUTE_COL,
        how="left",
    )
    merged[[LOAD_PATTERN_COL, PV_PATTERN_COL, WIND_PATTERN_COL]] = (
        merged[[LOAD_PATTERN_COL, PV_PATTERN_COL, WIND_PATTERN_COL]]
        .interpolate(method="linear")
        .ffill()
        .fillna(0.0)
    )
    return merged


def create_dynamic_network(config: Dict[str, float], bus_df: pd.DataFrame):
    bus_df = _normalize_bus_dataframe(bus_df)
    bus_count = len(bus_df)
    net = pp.create_empty_network()

    bus_hv = pp.create_bus(net, vn_kv=154.0, name="154kV Grid")
    buses = [pp.create_bus(net, vn_kv=22.9, name=f"Bus {i}") for i in range(bus_count + 1)]
    pp.create_ext_grid(net, bus=bus_hv, vm_pu=1.0)

    pp.create_transformer_from_parameters(
        net,
        hv_bus=bus_hv,
        lv_bus=buses[0],
        sn_mva=45.0,
        vn_hv_kv=154.0,
        vn_lv_kv=22.9,
        vk_percent=10.0,
        vkr_percent=0.5,
        pfe_kw=10.0,
        i0_percent=0.1,
        tap_step_percent=float(config["oltc_step"]),
        tap_pos=0,
        tap_min=-8,
        tap_max=8,
        tap_side="hv",
    )

    pp.create_line_from_parameters(
        net,
        from_bus=buses[0],
        to_bus=buses[1],
        length_km=float(config["len_01"]),
        r_ohm_per_km=float(config["cncv_r"]),
        x_ohm_per_km=float(config["cncv_x"]),
        c_nf_per_km=350.0,
        max_i_ka=0.53,
    )

    # Ensure config has enough lengths, default to 1.0 if missing
    lengths = []
    for i in range(bus_count - 1):
        key = f"len_{i+1}{i+2}"
        lengths.append(config.get(key, 1.0))

    for i in range(bus_count - 1):
        pp.create_line_from_parameters(
            net,
            from_bus=buses[i + 1],
            to_bus=buses[i + 2],
            length_km=float(lengths[i]),
            r_ohm_per_km=float(config["acsr_r"]),
            x_ohm_per_km=float(config["acsr_x"]),
            c_nf_per_km=10.0,
            max_i_ka=0.38,
        )

    for i in range(bus_count):
        bus_idx = buses[i + 1]
        pp.create_load(net, bus=bus_idx, p_mw=0.0, q_mvar=0.0, name=f"Load_{i + 1}")
        pp.create_sgen(
            net,
            bus=bus_idx,
            p_mw=0.0,
            q_mvar=0.0,
            sn_mva=max(float(bus_df.at[i, PV_P_COL]) * 1.2, 1.0),
            name=f"PV_{i + 1}",
        )
        pp.create_sgen(
            net,
            bus=bus_idx,
            p_mw=0.0,
            q_mvar=0.0,
            sn_mva=max(float(bus_df.at[i, WIND_P_COL]) * 1.2, 1.0),
            name=f"Wind_{i + 1}",
        )
        pp.create_storage(
            net,
            bus=bus_idx,
            p_mw=0.0,
            q_mvar=0.0,
            max_e_mwh=max(float(bus_df.at[i, ESS_CAP_COL]), 1.0),
            name=f"ESS_{i + 1}",
        )

    return net


def _safe_runpp(net, is_first: bool = False) -> bool:
    try:
        # Recycle mechanism skips heavy matrix building (Ybus/ppc) if topology hasn't fundamentally changed.
        # trafo=False means we still allow OLTC tap changes to recalculate the transformer admittance.
        # This provides a 10x~15x speedup over standard NR solving.
        if is_first:
            pp.runpp(net, numba=HAS_NUMBA)
        else:
            pp.runpp(net, numba=HAS_NUMBA, recycle=dict(trafo=False, gen=False, bus_pq=True))
        return True
    except Exception:
        res_bus = getattr(net, "res_bus", pd.DataFrame())
        if res_bus.empty or "vm_pu" not in res_bus.columns:
            net.res_bus = pd.DataFrame(1.0, index=net.bus.index, columns=["vm_pu", "va_degree"])
        return False


def _get_bus_voltage(net, bus_idx: int) -> float:
    res_bus = getattr(net, "res_bus", pd.DataFrame())
    if "vm_pu" in res_bus.columns and bus_idx in res_bus.index:
        return float(res_bus.vm_pu.at[bus_idx])
    return 1.0


def run_daily_simulation(
    config: Dict[str, float],
    bus_df: pd.DataFrame,
    time_df: pd.DataFrame,
    load_scale: float = 1.0,
    stop_voltage: Optional[float] = None,
    ess_efficiency: float = 0.95,
    total_minutes: int = 24 * 60,
    time_step_mins: int = 1,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    normalized_bus = _normalize_bus_dataframe(bus_df)
    bus_count = len(normalized_bus)
    sim_time = prepare_time_profile(time_df, total_minutes=total_minutes, time_step_mins=time_step_mins)
    minute_points = sim_time[MINUTE_COL].astype(int).tolist()
    sim_time = sim_time.set_index(MINUTE_COL)

    net = create_dynamic_network(config, normalized_bus)

    ess_efficiency = float(np.clip(ess_efficiency, 1e-6, 1.0))
    load_scale = float(load_scale)
    time_step_mins = int(max(1, time_step_mins))

    bus_map = {i + 1: int(net.bus.index[net.bus.name == f"Bus {i + 1}"][0]) for i in range(bus_count)}
    bus_lookup = {int(idx): str(name) for idx, name in net.bus["name"].items()}
    pv_indices = [int(net.sgen[net.sgen.name == f"PV_{i + 1}"].index[0]) for i in range(bus_count)]
    wind_indices = [int(net.sgen[net.sgen.name == f"Wind_{i + 1}"].index[0]) for i in range(bus_count)]
    storage_indices = [int(net.storage[net.storage.name == f"ESS_{i + 1}"].index[0]) for i in range(bus_count)]

    history_v = {f"Bus {i}": [] for i in range(1, bus_count + 1)}
    history_tap = []
    history_soc = {f"Bus {i}": [] for i in range(1, bus_count + 1)}
    history_ess_p = {f"Bus {i}": [] for i in range(1, bus_count + 1)}
    history_min_v = []
    history_totals = []

    current_tap = 0
    oltc_timer_mins = 0.0
    current_soc = [float(config["ess_init_soc"])] * bus_count
    violation: Optional[Dict[str, Any]] = None
    global_min_voltage = float("inf")
    ess_charge_mwh = 0.0
    ess_discharge_mwh = 0.0

    for step_idx, minute in enumerate(minute_points):
        if progress_cb:
            progress_cb(minute, total_minutes)

        if step_idx == 0:
            delta_mins = float(time_step_mins)
        else:
            delta_mins = float(max(1, minute - minute_points[step_idx - 1]))
        delta_h = delta_mins / 60.0

        load_pct = float(sim_time.at[minute, LOAD_PATTERN_COL]) / 100.0 * load_scale
        pv_pct = float(sim_time.at[minute, PV_PATTERN_COL]) / 100.0
        wind_pct = float(sim_time.at[minute, WIND_PATTERN_COL]) / 100.0

        for i in range(bus_count):
            net.load.p_mw.at[i] = float(normalized_bus.at[i, LOAD_P_COL]) * load_pct
            net.load.q_mvar.at[i] = float(normalized_bus.at[i, LOAD_Q_COL]) * load_pct
            net.sgen.p_mw.at[pv_indices[i]] = float(normalized_bus.at[i, PV_P_COL]) * pv_pct
            net.sgen.p_mw.at[wind_indices[i]] = float(normalized_bus.at[i, WIND_P_COL]) * wind_pct
            net.storage.p_mw.at[storage_indices[i]] = 0.0

        _safe_runpp(net, is_first=(step_idx == 0))

        step_charge_mw = 0.0
        step_discharge_mw = 0.0

        for i in range(bus_count):
            bus_idx = bus_map[i + 1]
            v_bus = _get_bus_voltage(net, bus_idx)
            ess_max = float(normalized_bus.at[i, ESS_MAX_COL])
            ess_cap = float(normalized_bus.at[i, ESS_CAP_COL])
            ess_p = 0.0

            if ess_max > 0.0 and ess_cap > 0.0:
                if v_bus > float(config["ess_target_v"]) and current_soc[i] < 100.0:
                    ess_p = ess_max
                elif v_bus < float(config["ess_discharge_v"]) and current_soc[i] > 0.0:
                    ess_p = -ess_max

                if ess_p != 0.0:
                    delta_soc = (ess_p * delta_h / ess_cap) * 100.0
                    if ess_p > 0.0:
                        delta_soc *= ess_efficiency
                        step_charge_mw += ess_p
                    else:
                        delta_soc /= ess_efficiency
                        step_discharge_mw += -ess_p
                    current_soc[i] = float(np.clip(current_soc[i] + delta_soc, 0.0, 100.0))

                net.storage.p_mw.at[storage_indices[i]] = ess_p

            history_soc[f"Bus {i + 1}"].append(current_soc[i])
            history_ess_p[f"Bus {i + 1}"].append(ess_p)

        ess_charge_mwh += step_charge_mw * delta_h
        ess_discharge_mwh += step_discharge_mw * delta_h

        _safe_runpp(net)

        bus5_idx = bus_map[5]
        v_bus5 = _get_bus_voltage(net, bus5_idx)
        if v_bus5 > float(config["v_upper_limit"]):
            if current_tap < 8:
                oltc_timer_mins += delta_mins
                if oltc_timer_mins >= float(config["oltc_delay_mins"]):
                    current_tap += 1
                    oltc_timer_mins = 0.0
            else:
                oltc_timer_mins = 0.0
        elif v_bus5 < float(config["v_lower_limit"]):
            if current_tap > -8:
                oltc_timer_mins += delta_mins
                if oltc_timer_mins >= float(config["oltc_delay_mins"]):
                    current_tap -= 1
                    oltc_timer_mins = 0.0
            else:
                oltc_timer_mins = 0.0
        else:
            oltc_timer_mins = 0.0

        net.trafo.tap_pos.at[0] = current_tap

        for i in range(bus_count):
            idx = bus_map[i + 1]
            history_v[f"Bus {i + 1}"].append(_get_bus_voltage(net, idx))
        history_tap.append(current_tap)

        res_bus = getattr(net, "res_bus", pd.DataFrame())
        if "vm_pu" in res_bus.columns and not res_bus.empty:
            minute_min_v = float(res_bus.vm_pu.min())
            min_v_bus_idx = int(res_bus.vm_pu.idxmin())
        else:
            minute_min_v = np.nan
            min_v_bus_idx = -1

        history_min_v.append(minute_min_v)
        if np.isfinite(minute_min_v):
            global_min_voltage = min(global_min_voltage, minute_min_v)

        total_load = float(net.load.p_mw.sum())
        total_pv = float(net.sgen.loc[pv_indices, "p_mw"].sum())
        total_wind = float(net.sgen.loc[wind_indices, "p_mw"].sum())
        total_ess = float(net.storage.loc[storage_indices, "p_mw"].sum())
        total_net = total_load + total_ess - total_pv - total_wind
        snapshot = {
            "load_mw": total_load,
            "pv_mw": total_pv,
            "wind_mw": total_wind,
            "ess_mw": total_ess,
            "net_mw": total_net,
        }
        history_totals.append(snapshot)

        if stop_voltage is not None and np.isfinite(minute_min_v) and minute_min_v <= float(stop_voltage):
            violation = {
                "minute": minute,
                "time": format_minute(minute),
                "voltage": minute_min_v,
                "bus_index": min_v_bus_idx,
                "bus_name": bus_lookup.get(min_v_bus_idx, f"BusIndex {min_v_bus_idx}"),
                "load_scale": load_scale,
                "load_scale_percent": load_scale * 100.0,
                "totals": snapshot,
            }
            break

    points_recorded = len(history_tap)
    minute_recorded = minute_points[:points_recorded]
    time_index = [format_minute(m) for m in minute_recorded]

    df_v = pd.DataFrame(history_v, index=time_index)
    df_tap = pd.DataFrame({"OLTC Tap": history_tap}, index=time_index)
    df_soc = pd.DataFrame(history_soc, index=time_index)
    df_ess_p = pd.DataFrame(history_ess_p, index=time_index)
    df_min_v = pd.DataFrame({"Min Voltage (p.u.)": history_min_v}, index=time_index)
    df_totals = pd.DataFrame(history_totals, index=time_index)

    if len(history_tap) > 1:
        oltc_moves = int(np.count_nonzero(np.diff(np.array(history_tap))))
    else:
        oltc_moves = 0

    soc_min = float(df_soc.min().min()) if not df_soc.empty else np.nan
    soc_max = float(df_soc.max().max()) if not df_soc.empty else np.nan

    worst_case = None
    if not df_min_v.empty and np.isfinite(df_min_v["Min Voltage (p.u.)"]).any():
        worst_label = str(df_min_v["Min Voltage (p.u.)"].astype(float).idxmin())
        hh, mm = worst_label.split(":")
        worst_minute = int(hh) * 60 + int(mm)
        worst_case = {
            "minute": worst_minute,
            "time": format_minute(worst_minute),
            "voltage": float(df_min_v["Min Voltage (p.u.)"].min()),
        }

    results = {
        "df_v": df_v,
        "df_tap": df_tap,
        "df_soc": df_soc,
        "df_ess_p": df_ess_p,
        "df_min_v": df_min_v,
        "df_totals": df_totals,
        "time_index": time_index,
    }
    events = {
        "violation": violation,
        "stopped_early": violation is not None,
        "global_min_voltage": global_min_voltage if np.isfinite(global_min_voltage) else np.nan,
        "oltc_moves": oltc_moves,
        "final_tap": int(history_tap[-1]) if history_tap else 0,
        "ess_charge_mwh": float(ess_charge_mwh),
        "ess_discharge_mwh": float(ess_discharge_mwh),
        "soc_min": soc_min,
        "soc_max": soc_max,
        "total_points_simulated": points_recorded,
        "time_step_mins": time_step_mins,
        "worst_case": worst_case,
    }
    return results, events


def build_excel_bytes(sim_results: Dict[str, pd.DataFrame]) -> bytes:
    buffer = io.BytesIO()
    engines = ["xlsxwriter", "openpyxl", None]
    last_error: Optional[Exception] = None

    for engine in engines:
        try:
            buffer.seek(0)
            buffer.truncate(0)
            if engine is None:
                writer = pd.ExcelWriter(buffer)
            else:
                writer = pd.ExcelWriter(buffer, engine=engine)
            with writer:
                sim_results["df_v"].to_excel(writer, sheet_name="Voltage_PU")
                sim_results["df_tap"].to_excel(writer, sheet_name="OLTC_Tap")
                sim_results["df_soc"].to_excel(writer, sheet_name="ESS_SOC")
                sim_results["df_ess_p"].to_excel(writer, sheet_name="ESS_Power_MW")
                sim_results["df_min_v"].to_excel(writer, sheet_name="Min_Voltage")
                sim_results["df_totals"].to_excel(writer, sheet_name="Power_Summary")
            return buffer.getvalue()
        except Exception as exc:
            last_error = exc
            continue

    raise RuntimeError("Excel 파일 생성에 실패했습니다.") from last_error


def run_limit_search(
    config: Dict[str, float],
    bus_df: pd.DataFrame,
    time_df: pd.DataFrame,
    start_scale: float = 1.0,
    step: float = 0.1,
    max_scale: float = 5.0,
    threshold: float = 0.9,
    ess_efficiency: float = 0.95,
    time_step_mins: int = 10,
    total_minutes: int = 24 * 60,
    progress_cb: Optional[Callable[[int, int, float], None]] = None,
) -> Dict[str, Any]:
    scale = float(start_scale)
    step = float(step)
    max_scale = float(max_scale)
    threshold = float(threshold)

    runs = []
    last_results = None
    last_events = None
    violation = None

    while scale <= max_scale + 1e-9:
        def _nested_progress(minute: int, total: int):
            if progress_cb is not None:
                progress_cb(minute, total, scale)

        results, events = run_daily_simulation(
            config=config,
            bus_df=bus_df,
            time_df=time_df,
            load_scale=scale,
            stop_voltage=threshold,
            ess_efficiency=ess_efficiency,
            total_minutes=total_minutes,
            time_step_mins=time_step_mins,
            progress_cb=_nested_progress,
        )

        last_results = results
        last_events = events
        run_info = {
            "load_scale": scale,
            "load_scale_percent": scale * 100.0,
            "min_voltage": events.get("global_min_voltage", np.nan),
            "violation": events.get("violation") is not None,
        }
        runs.append(run_info)

        if events.get("violation") is not None:
            violation = events["violation"]
            break

        scale = round(scale + step, 10)

    found_scale = runs[-1]["load_scale"] if runs else start_scale

    return {
        "runs": runs,
        "found_scale": found_scale,
        "violation": violation,
        "last_results": last_results,
        "last_events": last_events,
        "threshold": threshold,
        "start_scale": start_scale,
        "step": step,
        "max_scale": max_scale,
        "time_step_mins": time_step_mins,
    }


def _extract_bus_number(bus_name: str, bus_index: int) -> str:
    text = str(bus_name)
    if "Bus" in text:
        parts = text.replace("_", " ").split()
        for i, token in enumerate(parts):
            if token.lower() == "bus" and i + 1 < len(parts) and parts[i + 1].isdigit():
                return parts[i + 1]
    return str(bus_index)


def _limit_recommendations(violation: Optional[Dict[str, Any]], events: Dict[str, Any]) -> list[str]:
    recommendations = []
    if violation is None:
        recommendations.append("최대 탐색 범위까지 임계치 미도달입니다. 필요 시 max_scale을 확대해 재탐색하세요.")
        recommendations.append("저전압 임계치가 과도하면 threshold를 소폭 조정해 현실적인 한계점을 확인하세요.")
        return recommendations

    totals = violation["totals"]
    renewable_ratio = (totals["pv_mw"] + totals["wind_mw"]) / max(totals["load_mw"], 1e-9)
    if renewable_ratio < 0.2:
        recommendations.append("임계 시점 신재생 기여도가 낮습니다. 피크 시간대 ESS 방전 우선 전략을 권장합니다.")
    else:
        recommendations.append("신재생 출력 변동이 존재합니다. OLTC 지연시간과 ESS 개입전압을 함께 튜닝하세요.")

    if abs(events.get("final_tap", 0)) >= 8:
        recommendations.append("OLTC 탭 한계 근접입니다. 탭 운전범위/지연시간을 재검토하세요.")
    elif events.get("oltc_moves", 0) >= 30:
        recommendations.append("OLTC 동작 빈도가 높습니다. 지연시간 상향 또는 ESS 임계전압 조정이 필요합니다.")

    if events.get("soc_min", 0.0) <= 5.0 or events.get("soc_max", 100.0) >= 95.0:
        recommendations.append("ESS SOC 포화가 발생했습니다. ESS 용량/출력 증설 또는 제어 기준 조정을 권장합니다.")

    if not recommendations:
        recommendations.append("현재 설정은 안정적입니다. 계절/부하 프로파일별 민감도 분석을 확장하세요.")
    return recommendations


def build_limit_report(search_result: Dict[str, Any]) -> str:
    violation = search_result.get("violation")
    events = search_result.get("last_events") or {}
    runs = search_result.get("runs") or []

    lines = []
    lines.append("# 자동 부하증분 시뮬레이션 보고서")
    lines.append("")
    lines.append("## 1) 탐색 설정")
    lines.append(f"- 시작 부하배율: {float(search_result.get('start_scale', 1.0)) * 100:.1f}%")
    lines.append(f"- 증가폭: {float(search_result.get('step', 0.1)) * 100:.1f}%")
    lines.append(f"- 최대 부하배율: {float(search_result.get('max_scale', 5.0)) * 100:.1f}%")
    lines.append(f"- 저전압 임계치: {float(search_result.get('threshold', 0.9)):.3f} p.u.")
    lines.append(f"- 시간 간격: {int(search_result.get('time_step_mins', 10))}분")
    lines.append("")

    lines.append("## 2) 결과 요약")
    if violation is not None:
        bus_number = _extract_bus_number(violation["bus_name"], int(violation["bus_index"]))
        lines.append(f"- 임계 도달 부하배율: {float(search_result.get('found_scale', 1.0)) * 100:.1f}%")
        lines.append(f"- 최초 위반 시각: {violation['time']} ({violation['minute']}분)")
        lines.append(f"- 문제 Bus: {bus_number} (원본: {violation['bus_name']}, index={violation['bus_index']})")
        lines.append(f"- 최소 전압: {violation['voltage']:.4f} p.u.")
        totals = violation["totals"]
        lines.append(f"- 총 부하: {totals['load_mw']:.3f} MW")
        lines.append(f"- 태양광/풍력: {totals['pv_mw']:.3f} / {totals['wind_mw']:.3f} MW")
        lines.append(f"- ESS 순출력(+충전/-방전): {totals['ess_mw']:.3f} MW")
        lines.append(f"- 순부하(Load+ESS-PV-Wind): {totals['net_mw']:.3f} MW")
    else:
        lines.append("- 최대 부하배율까지 임계치(<=threshold) 미도달")
        lines.append(f"- 관측 최소 전압: {events.get('global_min_voltage', float('nan')):.4f} p.u.")
    lines.append("")

    lines.append("## 3) 부하배율별 최소전압 추이")
    lines.append("| 부하배율(%) | 최소전압(p.u.) | 임계도달 |")
    lines.append("|---:|---:|:---:|")
    for run in runs:
        lines.append(
            f"| {run['load_scale_percent']:.1f} | {run['min_voltage']:.4f} | {'Y' if run['violation'] else 'N'} |"
        )
    lines.append("")

    lines.append("## 4) 제어기 동작 요약")
    lines.append(f"- OLTC 동작 횟수: {events.get('oltc_moves', 0)}")
    lines.append(f"- 최종 OLTC 탭: {events.get('final_tap', 0)}")
    lines.append(f"- ESS 충전 에너지: {events.get('ess_charge_mwh', 0.0):.3f} MWh")
    lines.append(f"- ESS 방전 에너지: {events.get('ess_discharge_mwh', 0.0):.3f} MWh")
    lines.append(f"- ESS SOC 범위: {events.get('soc_min', float('nan')):.2f}% ~ {events.get('soc_max', float('nan')):.2f}%")
    lines.append("")

    lines.append("## 5) 튜닝 제언")
    for rec in _limit_recommendations(violation, events):
        lines.append(f"- {rec}")

    return "\n".join(lines)


def write_limit_report(report_path: str, search_result: Dict[str, Any]) -> str:
    report_text = build_limit_report(search_result)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    return report_text

import zipfile
from xml.sax.saxutils import escape

SCENARIO_LOAD_INCREASE = "load_increase"
SCENARIO_RENEWABLE_INCREASE = "renewable_increase"
SCENARIO_RENEWABLE_BY_LOAD_LEVEL = "renewable_by_load_level"
LOAD_LEVEL_COL = "부하구간"

STATE_NORMAL = "NORMAL"
STATE_UNDERVOLTAGE = "UNDERVOLTAGE"
STATE_OVERVOLTAGE = "OVERVOLTAGE"
STATE_CONGESTION = "CONGESTION"


def scenario_label(scenario: str) -> str:
    labels = {
        SCENARIO_LOAD_INCREASE: "부하 증가 시뮬레이션",
        SCENARIO_RENEWABLE_INCREASE: "재생에너지 증가 시뮬레이션",
        SCENARIO_RENEWABLE_BY_LOAD_LEVEL: "부하구간별 재생에너지 증가 시뮬레이션",
    }
    return labels.get(scenario, scenario)


def _advanced_config(config: Dict[str, float]) -> Dict[str, float]:
    merged = default_config()
    merged.update(config)
    merged.setdefault("voltage_min_limit", 0.94)
    merged.setdefault("voltage_max_limit", 1.06)
    merged.setdefault("voltage_low_off", 0.955)
    merged.setdefault("voltage_high_off", 1.045)
    merged.setdefault("line_limit_mva", 12.0)
    merged.setdefault("line_return_mva", 11.4)
    merged.setdefault("oltc_return_delay_mins", 20)
    merged.setdefault("ess_min_soc", 10.0)
    merged.setdefault("ess_max_soc", 90.0)
    merged.setdefault("ess_p_gain", 16.0)
    merged.setdefault("ess_q_gain", 18.0)
    merged.setdefault("line_relief_gain", 3.0)
    merged.setdefault("ess_ramp_rate_mw_per_min", 0.2)
    merged.setdefault("low_load_upper_pct", 60.0)
    merged.setdefault("mid_load_upper_pct", 85.0)
    return merged


def _classify_load_level_adv(load_pct: float, config: Dict[str, float]) -> str:
    if load_pct <= float(config["low_load_upper_pct"]):
        return "경부하"
    if load_pct <= float(config["mid_load_upper_pct"]):
        return "중간부하"
    return "중부하"


def _prepare_time_profile_adv(
    time_df: pd.DataFrame,
    total_minutes: int,
    time_step_mins: int,
    config: Dict[str, float],
) -> pd.DataFrame:
    prepared = prepare_time_profile(time_df, total_minutes=total_minutes, time_step_mins=time_step_mins).copy()
    prepared[LOAD_LEVEL_COL] = prepared[LOAD_PATTERN_COL].apply(lambda x: _classify_load_level_adv(float(x), config))
    return prepared


def _line_metrics_adv(net) -> Tuple[Dict[str, float], float, str, float]:
    if getattr(net, "res_line", pd.DataFrame()).empty:
        return {}, 0.0, "", 0.0

    if "name" not in net.line.columns or net.line["name"].isna().all():
        net.line["name"] = [f"Line {i}" for i in range(len(net.line))]

    line_mva = {}
    worst_name = ""
    worst_mva = 0.0
    signed_p = 0.0
    for idx, name in net.line["name"].items():
        p_from = float(net.res_line.p_from_mw.at[idx]) if "p_from_mw" in net.res_line.columns else 0.0
        q_from = float(net.res_line.q_from_mvar.at[idx]) if "q_from_mvar" in net.res_line.columns else 0.0
        p_to = float(net.res_line.p_to_mw.at[idx]) if "p_to_mw" in net.res_line.columns else 0.0
        q_to = float(net.res_line.q_to_mvar.at[idx]) if "q_to_mvar" in net.res_line.columns else 0.0
        s_from = math.sqrt(p_from**2 + q_from**2)
        s_to = math.sqrt(p_to**2 + q_to**2)
        s_val = max(s_from, s_to)
        line_mva[str(name)] = s_val
        if s_val >= worst_mva:
            worst_mva = s_val
            worst_name = str(name)
            signed_p = p_from if abs(p_from) >= abs(p_to) else p_to
    return line_mva, worst_mva, worst_name, signed_p


def _evaluate_limits_adv(min_v: float, max_v: float, max_line_mva: float, config: Dict[str, float]) -> Tuple[bool, bool, bool]:
    voltage_ok = min_v >= float(config["voltage_min_limit"]) and max_v <= float(config["voltage_max_limit"])
    line_ok = max_line_mva <= float(config["line_limit_mva"])
    return voltage_ok, line_ok, bool(voltage_ok and line_ok)


def _determine_state_adv(prev_state: str, min_v: float, max_v: float, max_line_mva: float, config: Dict[str, float]) -> str:
    if prev_state == STATE_CONGESTION and max_line_mva > float(config["line_return_mva"]):
        return STATE_CONGESTION
    if prev_state == STATE_UNDERVOLTAGE and min_v < float(config["voltage_low_off"]):
        return STATE_UNDERVOLTAGE
    if prev_state == STATE_OVERVOLTAGE and max_v > float(config["voltage_high_off"]):
        return STATE_OVERVOLTAGE
    if max_line_mva > float(config["line_limit_mva"]):
        return STATE_CONGESTION
    if min_v < float(config["voltage_min_limit"]):
        return STATE_UNDERVOLTAGE
    if max_v > float(config["voltage_max_limit"]):
        return STATE_OVERVOLTAGE
    return STATE_NORMAL


def _ramp_to_zero_adv(value: float, ramp_limit: float) -> float:
    if abs(value) <= ramp_limit:
        return 0.0
    return value - math.copysign(ramp_limit, value)


def _build_docx_bytes(title: str, paragraphs: list[str]) -> bytes:
    def paragraph_xml(text: str) -> str:
        return f'<w:p><w:r><w:t xml:space="preserve">{escape(text)}</w:t></w:r></w:p>'

    body = [f'<w:p><w:r><w:rPr><w:b/></w:rPr><w:t xml:space="preserve">{escape(title)}</w:t></w:r></w:p>']
    body.extend(paragraph_xml(p) for p in paragraphs)
    document_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        '<w:body>'
        + ''.join(body)
        + '<w:sectPr><w:pgSz w:w="12240" w:h="15840"/><w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440" w:header="720" w:footer="720" w:gutter="0"/></w:sectPr>'
        '</w:body></w:document>'
    )
    content_types = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>'
    rels = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>'
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels)
        zf.writestr("word/document.xml", document_xml)
    return buffer.getvalue()



