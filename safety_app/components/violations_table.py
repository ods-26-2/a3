import streamlit as st
import pandas as pd


def render_violations_table_component(df: pd.DataFrame, clips_page=None, mock_mode=True, alerts_page=None):
	st.subheader('Últimas Violações de Zona')
	st.caption('Dados simulados: IDs sintéticos compatíveis com S2.' if mock_mode else 'Eventos S2 classificados pela matriz contextual e persistidos localmente no A3.')
	df_display = df.copy()
	if 'ID do evento' not in df_display:
		df_display.insert(0, 'ID do evento', None)
	df_display['Horário da violação'] = pd.to_datetime(df_display['Horário da violação']).dt.strftime('%Y-%m-%d %H:%M:%S')
	df_display['ID do evento'] = df_display['ID do evento'].fillna('—')
	selection = st.dataframe(df_display, use_container_width=True, hide_index=True,
		on_select='rerun', selection_mode='single-row', key='a3_selected_violation')
	rows = selection.selection.rows
	if rows:
		event_id = df.iloc[rows[0]].get('ID do evento')
		if isinstance(event_id, str) and event_id.strip():
			if st.button(f'🎬 Abrir clipe do evento {event_id[:12]}…', key='open_event_clip'):
				st.session_state['a3_clip_event_id'] = event_id
				st.session_state['a3_mock_clips_enabled'] = bool(mock_mode)
				st.session_state['a3_clip_auto_lookup'] = True
				st.session_state.pop('a3_s4_clips', None)
				if clips_page is not None:
					st.switch_page(clips_page)
			if alerts_page is not None and st.button(f'🚨 Ver alertas do evento {event_id[:12]}…', key='open_event_alerts'):
				st.session_state['a3_alert_event_id'] = event_id
				st.switch_page(alerts_page)
		else:
			st.info('Registro sem event_id: não há um evento qualificado associado.')
