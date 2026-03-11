import argparse
from pathlib import Path

import pandas as pd

from coordinated_engine import (
    CONTROL_CASE_OLTC_ESS,
    SCENARIO_BOTH_INCREASE,
    SCENARIO_LOAD_INCREASE,
    SCENARIO_MODE_ESS_SIZING,
    SCENARIO_MODE_HOSTING_CAPACITY,
    SCENARIO_MODE_LOAD_PV_MAP,
    SCENARIO_RENEWABLE_BY_LOAD_LEVEL,
    SCENARIO_RENEWABLE_INCREASE,
    build_batch_summary_csv_bytes,
    build_batch_summary_excel_bytes,
    generate_scenarios,
    run_batch_simulations,
    run_sensitivity_search,
    scenario_label,
    scenario_mode_label,
    write_word_report,
)
from sim_engine import default_bus_dataframe, default_config, default_time_profile_dataframe, load_time_profile


def parse_args():
    parser = argparse.ArgumentParser(description="OLTC-ESS automation runner")
    parser.add_argument("--profile", type=str, default=None, help="Time-profile file path (CSV/XLSX)")
    parser.add_argument(
        "--scenario",
        type=str,
        default=SCENARIO_LOAD_INCREASE,
        choices=[SCENARIO_LOAD_INCREASE, SCENARIO_RENEWABLE_INCREASE, SCENARIO_BOTH_INCREASE, SCENARIO_RENEWABLE_BY_LOAD_LEVEL],
        help="Sensitivity scenario for the existing limit search mode",
    )
    parser.add_argument("--start-scale", type=float, default=1.0, help="Start scale for sensitivity mode")
    parser.add_argument("--step", type=float, default=0.1, help="Step size for sensitivity mode")
    parser.add_argument("--max-scale", type=float, default=3.0, help="Max scale for sensitivity mode")
    parser.add_argument("--ess-eff", type=float, default=0.95, help="ESS round-trip efficiency")
    parser.add_argument("--time-step", type=int, default=10, help="Simulation time step in minutes")
    parser.add_argument("--report-path", type=str, default="simulation_report.docx", help="Word report output path for sensitivity mode")

    parser.add_argument("--batch-mode", action="store_true", help="Run scenario generation and batch execution")
    parser.add_argument(
        "--batch-scenario-mode",
        type=str,
        default=SCENARIO_MODE_HOSTING_CAPACITY,
        choices=[SCENARIO_MODE_HOSTING_CAPACITY, SCENARIO_MODE_LOAD_PV_MAP, SCENARIO_MODE_ESS_SIZING],
        help="Research-driven batch scenario mode",
    )
    parser.add_argument("--pv-penetration", type=str, default="0.8,1.0,1.2,1.4,1.6", help="PV penetration values or range")
    parser.add_argument("--ess-size", type=str, default="0.0,0.5,1.0,1.5", help="ESS size multipliers")
    parser.add_argument("--ess-location", type=str, default="5", help="ESS location or location sweep, e.g. 5 or 3,4,5")
    parser.add_argument("--load-growth", type=str, default="0.8,1.0,1.2,1.4", help="Load growth values or range")
    parser.add_argument("--control-case", type=str, default=CONTROL_CASE_OLTC_ESS, help="Control case for batch modes")
    parser.add_argument("--base-pv-penetration", type=float, default=1.6, help="Base PV penetration for ESS sizing mode")
    parser.add_argument("--base-load-growth", type=float, default=1.0, help="Base load growth for ESS sizing mode")
    parser.add_argument("--max-workers", type=int, default=None, help="Max workers for parallel batch execution")
    parser.add_argument("--serial", action="store_true", help="Force serial execution for batch mode")
    parser.add_argument("--include-timeseries", action="store_true", help="Store detailed outputs in batch mode (forces serial)")
    parser.add_argument("--batch-csv-path", type=str, default="batch_summary.csv", help="CSV output path for batch summary")
    parser.add_argument("--batch-xlsx-path", type=str, default="batch_summary.xlsx", help="Excel output path for batch summary")
    return parser.parse_args()


def write_bytes(path: str, data: bytes) -> str:
    Path(path).write_bytes(data)
    return path


def main():
    args = parse_args()

    config = default_config()
    config["ess_efficiency"] = float(args.ess_eff)
    config["time_step_mins"] = int(args.time_step)
    config["voltage_min_limit"] = 0.94
    config["voltage_max_limit"] = 1.06
    config["line_limit_mva"] = 12.0
    config["line_return_mva"] = 11.4
    config["ess_power_mw"] = 5.0
    config["ess_capacity_mwh"] = 15.0
    config["ess_bus_number"] = 5

    bus_df = default_bus_dataframe()
    time_df = load_time_profile(args.profile) if args.profile else default_time_profile_dataframe()
    if args.profile:
        print(f"[INFO] Loaded time profile: {args.profile}")
    else:
        print("[INFO] Using default time profile")

    if args.batch_mode:
        settings = {
            "mode": args.batch_scenario_mode,
            "pv_penetration": args.pv_penetration,
            "ess_size": args.ess_size,
            "ess_location": args.ess_location,
            "load_growth": args.load_growth,
            "control_case": args.control_case,
            "base_pv_penetration": float(args.base_pv_penetration),
            "base_load_growth": float(args.base_load_growth),
            "default_ess_location": int(config["ess_bus_number"]),
            "base_ess_power_mw": float(config["ess_power_mw"]),
            "base_ess_capacity_mwh": float(config["ess_capacity_mwh"]),
        }
        scenarios = generate_scenarios(args.batch_scenario_mode, settings)
        print(f"[BATCH] mode={scenario_mode_label(args.batch_scenario_mode)} | Generated {len(scenarios)} scenarios")

        batch_result = run_batch_simulations(
            scenarios=scenarios,
            config=config,
            bus_df=bus_df,
            time_df=time_df,
            ess_efficiency=float(args.ess_eff),
            time_step_mins=int(args.time_step),
            total_minutes=24 * 60,
            max_workers=args.max_workers,
            parallel=not args.serial,
            include_timeseries=bool(args.include_timeseries),
        )

        summary_df = pd.DataFrame(batch_result.get("summary_records", []))
        print(
            f"[BATCH] mode={batch_result.get('execution_mode')} | "
            f"workers={batch_result.get('max_workers')} | elapsed={float(batch_result.get('elapsed_sec', 0.0)):.2f}s"
        )
        if batch_result.get("fallback_reason"):
            print(f"[BATCH] note={batch_result['fallback_reason']}")
        if not summary_df.empty:
            print(summary_df.to_string(index=False))

        csv_path = write_bytes(args.batch_csv_path, build_batch_summary_csv_bytes(batch_result))
        xlsx_path = write_bytes(args.batch_xlsx_path, build_batch_summary_excel_bytes(batch_result))
        print(f"[EXPORT] CSV summary: {csv_path}")
        print(f"[EXPORT] Excel summary: {xlsx_path}")
        return

    search_result = run_sensitivity_search(
        config=config,
        bus_df=bus_df,
        time_df=time_df,
        scenario=args.scenario,
        start_scale=float(args.start_scale),
        step=float(args.step),
        max_scale=float(args.max_scale),
        ess_efficiency=float(args.ess_eff),
        time_step_mins=int(args.time_step),
        total_minutes=24 * 60,
    )

    print(f"[SCENARIO] {scenario_label(args.scenario)}")
    runs_df = pd.DataFrame(search_result["runs"])
    for _, run in runs_df.iterrows():
        print(
            f"[RUN] 증가배율={float(run['sweep_scale']):.2f} ({float(run['sweep_percent']):.1f}%) | "
            f"Vmin={float(run['min_voltage']):.4f} p.u. | "
            f"Vmax={float(run['max_voltage']):.4f} p.u. | "
            f"Line={float(run['max_line_mva']):.3f} MVA | "
            f"전압={'OK' if bool(run['voltage_ok']) else 'NG'} | "
            f"선로={'OK' if bool(run['line_ok']) else 'NG'}"
        )

    first_failure = search_result.get("first_failure")
    if first_failure:
        print(
            f"[FIRST_FAIL] 증가배율={float(first_failure['sweep_scale']):.2f} | "
            f"Vmin={float(first_failure['min_voltage']):.4f} | "
            f"Vmax={float(first_failure['max_voltage']):.4f} | "
            f"Line={float(first_failure['max_line_mva']):.3f}"
        )
    else:
        print("[DONE] 설정 범위 내 허용치 이탈 없음")

    write_word_report(args.report_path, search_result)
    print(f"[REPORT] 보고서 생성 완료: {args.report_path}")


if __name__ == "__main__":
    main()
