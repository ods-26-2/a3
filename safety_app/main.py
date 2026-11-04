import streamlit as st
from theme import CUSTOM_CSS
from services.violations import get_mock_violations
from components.layout import render_header
from pages.zone_editor import render_zone_editor_page
from pages.dashboard import render_dashboard_page

def main():
    st.set_page_config(page_title="Sistema de Monitoramento Zonas de Risco", layout="wide")
    
    # Aplica CSS do arquivo de temas
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    
    # Busca dados mockados do serviço
    df_violations = get_mock_violations()
    
    # Monta a tela inicial unificada
    render_header()
    render_zone_editor_page()
    st.divider()
    render_dashboard_page(df_violations)

if __name__ == "__main__":
    main()