import streamlit as st
from services.zones import confirm_zone_config, clear_zone_config

def render_zone_editor_page():
    """Renderiza o componente da área de edição da Zona de Risco."""
    st.subheader("Editor de Zona de Risco")
    
    st.markdown('<div class="editor-square"></div>', unsafe_allow_html=True)
    
    col1, col2, _ = st.columns([2, 2, 8])
    with col1:
        if st.button("Confirmar", use_container_width=True, type="primary"):
            if confirm_zone_config():
                st.success("Configuração de zona confirmada.")
    with col2:
        if st.button("Limpar", use_container_width=True):
            if clear_zone_config():
                st.info("Área do editor limpa.")