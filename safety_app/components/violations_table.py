import streamlit as st
import pandas as pd

def render_violations_table_component(df: pd.DataFrame):
    """Renderiza a tabela de violações brancas registrada."""
    st.subheader("Últimas Violações de Zona")
    
    df_display = df.copy()
    df_display["Horário da violação"] = df_display["Horário da violação"].dt.strftime("%Y-%m-%d %H:%M:%S")
    
    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True
    )