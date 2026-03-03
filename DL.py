import streamlit as st
import pandapower as pp
import pandas as pd
import numpy as np
import plotly.express as px
import io

st.set_page_config(page_title="협조 제어 동적 시뮬레이터", layout="wide")

st.title("⚡ 24시간 동적 시뮬레이션: OLTC & ESS 협조 제어")

# ==========================================
# 0. R + jX 문자열 파싱 함수
# ==========================================
def parse_impedance(z_str):
    try:
        z_str = z_str.replace(" ", "").lower()
        parts = z_str.split('+')
        r = float(parts[0])
        x = float(parts[1].replace('j', '').replace('i', ''))
        return r, x
    except:
        st.error(f"임피던스 입력 형식 오류 ({z_str}). 'R + jX' 형식으로 입력하세요.")
        return 0.0, 0.0

# ==========================================
# 1. 왼쪽 사이드바 설정 (선로 및 제어 알고리즘)
# ==========================================
st.sidebar.header("📍 모선간 선로 파라미터")
with st.sidebar.expander("선로 길이 설정 (km)", expanded=False):
    len_01 = st.number_input("Line 0-1 (CNCV)", value=3.0, step=0.5)
    len_12 = st.number_input("Line 1-2 (ACSR)", value=1.0, step=0.5)
    len_23 = st.number_input("Line 2-3 (ACSR)", value=1.0, step=0.5)
    len_34 = st.number_input("Line 3-4 (ACSR)", value=1.0, step=0.5)
    len_45 = st.number_input("Line 4-5 (ACSR)", value=1.0, step=0.5)

with st.sidebar.expander("선로 임피던스 (Ω/km)", expanded=False):
    st.caption("형식: R + jX")
    z_cncv_str = st.text_input("CNCV 325mm²", value="0.07 + j0.12")
    z_acsr_str = st.text_input("ACSR 160mm²", value="0.18 + j0.39")
    cncv_r, cncv_x = parse_impedance(z_cncv_str)
    acsr_r, acsr_x = parse_impedance(z_acsr_str)

st.sidebar.header("⚙️ 변전소 OLTC 특성 설정")
st.caption("문헌 기반 배전용 변압기 표준 특성")
oltc_step = st.sidebar.number_input("탭당 전압 변동률 (%)", value=1.25, step=0.05, help="보통 1탭 변경 시 1.25%의 전압이 조절됩니다.")
oltc_delay_mins = st.sidebar.slider("OLTC 동작 시지연 (분)", 1, 10, 3, help="과전압/저전압이 이 시간 이상 지속되어야 기계적 탭 동작이 발생합니다.")
v_upper_limit = st.sidebar.number_input("동작 상한 전압 (p.u.)", value=1.05, step=0.01, help="이 전압을 넘으면 OLTC가 전압을 낮추려 시도합니다.")
v_lower_limit = st.sidebar.number_input("동작 하한 전압 (p.u.)", value=0.95, step=0.01, help="이 전압 밑으로 떨어지면 OLTC가 전압을 높이려 시도합니다.")

st.sidebar.header("🔋 ESS 제어 설정")
ess_init_soc = st.sidebar.slider("ESS 초기 SOC (%)", 0.0, 100.0, 20.0, step=5.0)
ess_target_v = st.sidebar.number_input("ESS 충전 개입 목표 전압 (p.u.)", value=1.04, step=0.01, help="전압이 상한선에 도달하기 전, 이 전압을 넘으면 ESS가 즉시 충전을 시작하여 전압 상승을 억제합니다.")
ess_discharge_v = st.sidebar.number_input("ESS 방전 개입 목표 전압 (p.u.)", value=0.96, step=0.01)

# ==========================================
# 2. 메인 화면: 계통 파라미터 및 프로파일 입력
# ==========================================
st.header("1. 배전계통 모델링 및 데이터 입력")

st.subheader("🏢 모선별 연계 용량 설정 (MW, MVar, MWh)")
bus_data = {
    "모선": ["Bus 1", "Bus 2", "Bus 3", "Bus 4", "Bus 5"],
    "Load_P": [2.0, 2.0, 2.0, 2.0, 2.0],
    "Load_Q": [0.2, 0.2, 0.2, 0.2, 0.2],
    "PV_P": [0.0, 0.0, 2.0, 4.0, 8.0],
    "Wind_P": [0.0, 5.0, 0.0, 0.0, 0.0],
    "ESS_최대출력": [0.0, 0.0, 0.0, 2.0, 5.0],
    "ESS_용량": [0.0, 0.0, 0.0, 5.0, 15.0]
}
df_bus = pd.DataFrame(bus_data)
edited_df_bus = st.data_editor(df_bus, hide_index=True, width='stretch')

st.subheader("네트워크 구성도 (Topology)")
def get_components(idx):
    comps = []
    if edited_df_bus.at[idx, "Load_P"] > 0: comps.append("💡Load")
    if edited_df_bus.at[idx, "PV_P"] > 0: comps.append("☀️PV")
    if edited_df_bus.at[idx, "Wind_P"] > 0: comps.append("🎐Wind")
    if edited_df_bus.at[idx, "ESS_최대출력"] > 0: comps.append("🔋ESS")
    return "<br>".join(comps) if comps else "빈 모선"

diagram_html = f"""
<div style="display: flex; justify-content: space-between; align-items: center; background-color: #1E1E1E; padding: 20px; border-radius: 10px; color: white; text-align: center; font-size: 14px;">
    <div><b>Substation</b><br>OLTC</div>
    <div>▶<br><span style="color:#FFDD44; font-size:12px;">{len_01}km</span></div>
    <div><b>Bus 1</b><br><span style="font-size:12px;">{get_components(0)}</span></div>
    <div>▶<br><span style="color:#FFDD44; font-size:12px;">{len_12}km</span></div>
    <div><b>Bus 2</b><br><span style="font-size:12px;">{get_components(1)}</span></div>
    <div>▶<br><span style="color:#FFDD44; font-size:12px;">{len_23}km</span></div>
    <div><b>Bus 3</b><br><span style="font-size:12px;">{get_components(2)}</span></div>
    <div>▶<br><span style="color:#FFDD44; font-size:12px;">{len_34}km</span></div>
    <div><b>Bus 4</b><br><span style="font-size:12px;">{get_components(3)}</span></div>
    <div>▶<br><span style="color:#FFDD44; font-size:12px;">{len_45}km</span></div>
    <div><b>Bus 5</b><br><span style="font-size:12px;">{get_components(4)}</span></div>
</div>
"""
st.markdown(diagram_html, unsafe_allow_html=True)

st.markdown("---")
st.subheader("🕒 24시간 시계열 출력 패턴 입력")
st.markdown("엑셀 파일을 업로드하거나, 아래 기본 제공되는 24시간 표를 직접 수정하세요. (데이터 사이는 1분 단위로 자동 보간됩니다.)")

uploaded_file = st.file_uploader("📂 엑셀/CSV 파일 업로드 (선택사항)", type=["xlsx", "csv"], help="열 이름이 [시간 (Hour), 부하 패턴 (%), 태양광 패턴 (%), 풍력 패턴 (%)] 형식이면 가장 좋습니다.")

if uploaded_file:
    if uploaded_file.name.endswith('.csv'):
        df_time = pd.read_csv(uploaded_file)
    else:
        df_time = pd.read_excel(uploaded_file)
    st.success("파일이 성공적으로 로드되었습니다! 데이터를 확인 및 수정할 수 있습니다.")
else:
    # 24시간 기본 패턴 (예시)
    hours = list(range(25))
    load_pattern = [40, 38, 35, 35, 40, 45, 60, 75, 85, 90, 95, 100, 95, 90, 85, 80, 75, 80, 90, 95, 85, 70, 55, 45, 40]
    pv_pattern = [0]*6 + [5, 20, 50, 70, 90, 100, 95, 80, 50, 20, 5] + [0]*8
    wind_pattern = [30, 35, 40, 45, 40, 35, 30, 30, 25, 20, 20, 25, 30, 35, 40, 50, 60, 70, 65, 55, 45, 40, 35, 30, 30]
    
    time_data = {
        "시간 (Hour)": hours,
        "부하 패턴 (%)": load_pattern,
        "태양광 패턴 (%)": pv_pattern,
        "풍력 패턴 (%)": wind_pattern
    }
    df_time = pd.DataFrame(time_data)

edited_df_time = st.data_editor(df_time, num_rows="dynamic", hide_index=True, width='stretch')


# ==========================================
# 3. 네트워크 생성 및 시뮬레이션 로직
# ==========================================
def create_dynamic_network():
    net = pp.create_empty_network()
    
    bus_hv = pp.create_bus(net, vn_kv=154.0, name="154kV Grid")
    buses = [pp.create_bus(net, vn_kv=22.9, name=f"Bus {i}") for i in range(6)]
    pp.create_ext_grid(net, bus=bus_hv, vm_pu=1.0)

    # OLTC 반영
    pp.create_transformer_from_parameters(
        net, hv_bus=bus_hv, lv_bus=buses[0], sn_mva=45.0, vn_hv_kv=154.0, vn_lv_kv=22.9,
        vk_percent=10.0, vkr_percent=0.5, pfe_kw=10.0, i0_percent=0.1,
        tap_step_percent=oltc_step, tap_pos=0, tap_min=-9, tap_max=9, tap_side="hv"
    )

    pp.create_line_from_parameters(net, from_bus=buses[0], to_bus=buses[1], length_km=len_01, 
                                   r_ohm_per_km=cncv_r, x_ohm_per_km=cncv_x, c_nf_per_km=350.0, max_i_ka=0.53)
    lengths = [len_12, len_23, len_34, len_45]
    for i in range(4):
        pp.create_line_from_parameters(net, from_bus=buses[i+1], to_bus=buses[i+2], length_km=lengths[i], 
                                       r_ohm_per_km=acsr_r, x_ohm_per_km=acsr_x, c_nf_per_km=10.0, max_i_ka=0.38)
    
    for i in range(5):
        bus_idx = buses[i+1]
        pp.create_load(net, bus=bus_idx, p_mw=0, q_mvar=0, name=f"Load_{i+1}")
        pp.create_sgen(net, bus=bus_idx, p_mw=0, q_mvar=0, sn_mva=max(edited_df_bus.at[i, "PV_P"]*1.2, 1.0), name=f"PV_{i+1}")
        pp.create_sgen(net, bus=bus_idx, p_mw=0, q_mvar=0, sn_mva=max(edited_df_bus.at[i, "Wind_P"]*1.2, 1.0), name=f"Wind_{i+1}")
        pp.create_storage(net, bus=bus_idx, p_mw=0, q_mvar=0, max_e_mwh=max(edited_df_bus.at[i, "ESS_용량"], 1.0), name=f"ESS_{i+1}")
        
    return net

if st.button("🚀 24시간 동적 시뮬레이션 시작", type="primary"):
    # 24시간 = 1440분
    total_minutes = 24 * 60
    
    # 시간 데이터 분 단위 보간 로직
    # 만약 '시간 (Hour)' 컬럼이 있으면 분으로 변환, 없으면 인덱스를 활용
    if "시간 (Hour)" in edited_df_time.columns:
        edited_df_time["분 (Minute)"] = edited_df_time["시간 (Hour)"] * 60
    else:
        edited_df_time["분 (Minute)"] = edited_df_time.index * 60

    df_sim_time = pd.DataFrame({"분 (Minute)": range(total_minutes + 1)})
    df_sim_time = df_sim_time.merge(edited_df_time, on="분 (Minute)", how="left")
    df_sim_time = df_sim_time.interpolate(method='linear').ffill().fillna(0) # 'ffill' method argument is deprecated

    # 시뮬레이션 구동
    progress_bar = st.progress(0, text="초기화 중...")
    
    net = create_dynamic_network()
    
    history_v = {f"Bus {i}": [] for i in range(1, 6)}
    history_tap = []
    history_soc = {f"Bus {i}": [] for i in range(1, 6)}
    history_ess_p = {f"Bus {i}": [] for i in range(1, 6)}
    
    current_tap = 0
    oltc_timer = 0      
    current_soc = [ess_init_soc] * 5  # 사이드바에서 설정한 초기 SOC 적용

    # pandapower 내부 인덱스 찾기
    pv_indices = [net.sgen[net.sgen.name == f"PV_{i+1}"].index[0] for i in range(5)]
    wind_indices = [net.sgen[net.sgen.name == f"Wind_{i+1}"].index[0] for i in range(5)]
    bus_map = {i+1: net.bus.index[net.bus.name == f"Bus {i+1}"][0] for i in range(5)} # 정확한 버스 인덱스 맵핑

    for minute in range(total_minutes + 1):
        # UI 업데이트 (10분마다 갱신하여 속도 저하 방지)
        if minute % 10 == 0:
            progress_bar.progress(minute / total_minutes, text=f"조류해석 수행 중... ({minute}/{total_minutes} 분)")
        
        load_pct = df_sim_time.at[minute, "부하 패턴 (%)"] / 100.0
        pv_pct = df_sim_time.at[minute, "태양광 패턴 (%)"] / 100.0
        wind_pct = df_sim_time.at[minute, "풍력 패턴 (%)"] / 100.0 if "풍력 패턴 (%)" in df_sim_time.columns else 0.0
        
        # 1. 기기 설정 적용
        for i in range(5):
            net.load.p_mw.at[i] = edited_df_bus.at[i, "Load_P"] * load_pct
            net.load.q_mvar.at[i] = edited_df_bus.at[i, "Load_Q"] * load_pct
            net.sgen.p_mw.at[pv_indices[i]] = edited_df_bus.at[i, "PV_P"] * pv_pct
            net.sgen.p_mw.at[wind_indices[i]] = edited_df_bus.at[i, "Wind_P"] * wind_pct
            net.storage.p_mw.at[i] = 0.0 
            
        # 2. 예비 조류해석 (ESS 개입 전 확인)
        try:
            pp.runpp(net, numba=True)
        except:
            # 수렴 실패 시 이전 상태가 없을 수 있으므로 초기값 보장
            if minute == 0:
                net.res_bus = pd.DataFrame(1.0, index=net.bus.index, columns=["vm_pu", "va_degree"])
            pass 
        
        # 3. ESS 즉각 개입 로직 (1분 단위로 SOC 적산)
        for i in range(5):
            bus_idx = bus_map[i+1]
            v_bus = net.res_bus.vm_pu.at[bus_idx] 
            ess_max = edited_df_bus.at[i, "ESS_최대출력"]
            ess_cap = edited_df_bus.at[i, "ESS_용량"]
            ess_p = 0.0
            
            if ess_max > 0 and ess_cap > 0:
                if v_bus > ess_target_v and current_soc[i] < 100.0:
                    ess_p = ess_max 
                elif v_bus < ess_discharge_v and current_soc[i] > 0.0:
                    ess_p = -ess_max
                
                delta_soc = (ess_p * (1/60) / ess_cap) * 100.0
                current_soc[i] = max(0.0, min(100.0, current_soc[i] + delta_soc))
                net.storage.p_mw.at[i] = ess_p
            
            history_soc[f"Bus {i+1}"].append(current_soc[i])
            history_ess_p[f"Bus {i+1}"].append(ess_p)
        
        # 4. ESS 개입 후 최종 조류해석
        try:
            pp.runpp(net, numba=True)
        except:
            if minute == 0:
                net.res_bus = pd.DataFrame(1.0, index=net.bus.index, columns=["vm_pu", "va_degree"])
            pass
        
        # 5. OLTC 시지연 제어 로직 (말단 Bus 5 기준 전압 감시)
        bus5_idx = bus_map[5]
        v_bus5_final = net.res_bus.vm_pu.at[bus5_idx]
        
        if v_bus5_final > v_upper_limit:
            if current_tap < 9: # 최대 탭 한계치
                oltc_timer += 1
                if oltc_timer >= oltc_delay_mins:
                    current_tap += 1 
                    oltc_timer = 0
            else:
                oltc_timer = 0
        elif v_bus5_final < v_lower_limit:
            if current_tap > -9:
                oltc_timer += 1
                if oltc_timer >= oltc_delay_mins:
                    current_tap -= 1
                    oltc_timer = 0
            else:
                oltc_timer = 0
        else:
            oltc_timer = 0
            
        net.trafo.tap_pos.at[0] = current_tap
        
        # 데이터 저장
        for i in range(5):
            idx = bus_map[i+1]
            history_v[f"Bus {i+1}"].append(net.res_bus.vm_pu.at[idx])
        history_tap.append(current_tap)

    progress_bar.progress(1.0, text="시뮬레이션 완료!")

    # ==========================================
    # 4. 결과 시각화
    # ==========================================
    st.header("2. 24시간 협조 제어 시뮬레이션 결과")
    
    # x축 시간 레이블 생성 (00:00 ~ 24:00)
    time_index = [f"{m//60:02d}:{m%60:02d}" for m in range(total_minutes + 1)]
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("##### 📉 모선별 24시간 전압 프로파일 (p.u.)")
        df_v = pd.DataFrame(history_v, index=time_index)
        fig_v = px.line(df_v, labels={'index': '시간', 'value': 'Voltage (p.u.)', 'variable': '모선'})
        # Y축 0.7 ~ 1.2 고정 및 마우스 오버 시 전체 모선 데이터 표시
        fig_v.update_layout(yaxis_range=[0.7, 1.2], margin=dict(l=0, r=0, t=30, b=0), hovermode="x unified")
        st.plotly_chart(fig_v, width='stretch')
        
        st.markdown("##### ⚙️ 변전소 OLTC 탭 변화")
        df_tap = pd.DataFrame({"OLTC Tap": history_tap}, index=time_index)
        fig_tap = px.line(df_tap, labels={'index': '시간', 'value': 'Tap Position'})
        fig_tap.update_layout(margin=dict(l=0, r=0, t=30, b=0), hovermode="x unified")
        st.plotly_chart(fig_tap, width='stretch')

    with col2:
        st.markdown("##### 🔋 ESS SOC (%)")
        df_soc = pd.DataFrame(history_soc, index=time_index)
        fig_soc = px.line(df_soc, labels={'index': '시간', 'value': 'SOC (%)', 'variable': '모선'})
        # SOC는 0~100% 구간으로 고정
        fig_soc.update_layout(yaxis_range=[0, 100], margin=dict(l=0, r=0, t=30, b=0), hovermode="x unified")
        st.plotly_chart(fig_soc, width='stretch')
        
        st.markdown("##### ⚡ ESS 실시간 충방전 출력 (MW)")
        df_ess_p = pd.DataFrame(history_ess_p, index=time_index)
        fig_ess_p = px.line(df_ess_p, labels={'index': '시간', 'value': 'ESS 출력 (MW)', 'variable': '모선'})
        fig_ess_p.update_layout(margin=dict(l=0, r=0, t=30, b=0), hovermode="x unified")
        st.plotly_chart(fig_ess_p, width='stretch')

    # ==========================================
    # 5. 엑셀 다운로드 (BytesIO 메모리 버퍼 활용)
    # ==========================================
    st.markdown("---")
    st.subheader("📥 시뮬레이션 결과 다운로드")
    
    # 엑셀 파일 생성을 위한 메모리 버퍼
    buffer = io.BytesIO()
    
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df_v.to_excel(writer, sheet_name='Voltage_PU')
        df_tap.to_excel(writer, sheet_name='OLTC_Tap')
        df_soc.to_excel(writer, sheet_name='ESS_SOC')
        df_ess_p.to_excel(writer, sheet_name='ESS_Power_MW')
        
    st.download_button(
        label="📊 결과를 엑셀 파일로 저장 (.xlsx)",
        data=buffer.getvalue(),
        file_name="simulation_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary"
    )