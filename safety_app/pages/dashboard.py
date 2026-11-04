import streamlit as st
import pandas as pd
from components.violations_table import render_violations_table_component
from components.violations_chart import render_violations_chart_component

def render_dashboard_page(df_violations: pd.DataFrame):
    """Compõe a seção do dashboard integrando a tabela e o gráfico."""
    render_violations_table_component(df_violations)
    st.divider()
    render_violations_chart_component(df_violations)