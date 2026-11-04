import streamlit as st
from services.clips import fetch_recent_clips

def render_clips_page():
    """Página secundária para recuperar videoclipes (Mock)."""
    st.title("Retrieve video clip of recent violations")
    clips = fetch_recent_clips()
    st.write("Clipes disponíveis (Mock):", clips)