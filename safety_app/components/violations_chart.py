import streamlit as st
import pandas as pd
import plotly.express as px
from theme import PRIMARY_BLUE, SECONDARY_BLUE, LIGHT_BLUE

def render_violations_chart_component(df: pd.DataFrame):
    """Renderiza o gráfico acumulativo por hora e a visão em tabela pivot visual."""
    st.subheader("Violations per hour")
    st.caption(f"{len(df)} violations today, 00:00 to 10:59, by camera")
    
    df_chart = df.copy()
    df_chart['Hour'] = df_chart['Horário da violação'].dt.strftime('%H:00')
    grouped_df = df_chart.groupby(['Hour', 'Nome da câmera']).size().reset_index(name='Count')
    
    pivot_df = grouped_df.pivot(index='Hour', columns='Nome da câmera', values='Count').fillna(0).astype(int)
    pivot_df['Total'] = pivot_df.sum(axis=1)
    
    all_hours = [f"{str(i).zfill(2)}:00" for i in range(11)]
    pivot_df = pivot_df.reindex(all_hours, fill_value=0)
    
    tab_graph, tab_table = st.tabs(["Graph", "Table"])
    
    with tab_graph:
        fig = px.bar(
            grouped_df, 
            x="Hour", 
            y="Count", 
            color="Nome da câmera",
            category_orders={"Hour": all_hours},
            barmode="stack",
            color_discrete_sequence=[PRIMARY_BLUE, SECONDARY_BLUE, LIGHT_BLUE]
        )
        fig.update_layout(
            xaxis_title="", 
            yaxis_title="",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            legend_title_text="",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0)
        )
        st.plotly_chart(fig, use_container_width=True)
        
    with tab_table:
        st.dataframe(
            pivot_df.style.highlight_max(axis=0, color=PRIMARY_BLUE, subset=["Total"]),
            use_container_width=True
        )