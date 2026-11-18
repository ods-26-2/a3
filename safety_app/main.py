"""A3 Streamlit application. Mock source remains the default.

Only A3 code is changed. B6's compiled player and zone editor stay intact.
"""
import os

import streamlit as st

from theme import CUSTOM_CSS
from services.violations import get_mock_violations
from components.layout import render_header
from pages.zone_editor import render_zone_editor_page
from pages.dashboard import render_dashboard_page
from pages.clips import render_clips_page
from pages.risk_matrix import render_risk_matrix_page
from pages.contracts import render_contracts_page
from pages.alerts import render_alerts_page
from pages.simulator import render_simulator_page
from pages.connectivity import render_connectivity_page
from services.kafka_engine import A3KafkaEngine
from services.kafka_config import get_kafka_bootstrap_servers


def get_kafka_engine(brokers, publish_alerts):
	"""Create one optional consumer per Streamlit session and stop on opt-out."""
	config = (brokers, publish_alerts)
	engine = st.session_state.get('a3_live_engine')
	if engine is not None and st.session_state.get('a3_live_config') != config:
		engine.stop()
		engine = None
	if engine is None:
		engine = A3KafkaEngine(brokers=brokers, publish_alerts=publish_alerts)
		engine.start()
		st.session_state['a3_live_engine'] = engine
		st.session_state['a3_live_config'] = config
	return engine


def stop_kafka_engine():
	engine = st.session_state.pop('a3_live_engine', None)
	st.session_state.pop('a3_live_config', None)
	if engine is not None:
		engine.stop()


def show_zone_editor():
	render_header()
	render_zone_editor_page()


def show_clips():
	render_header()
	render_clips_page()


def show_risk_matrix():
	render_header()
	render_risk_matrix_page()


def show_contracts():
	render_header()
	render_contracts_page()


def main():
	st.set_page_config(page_title='Sistema de Monitoramento Zonas de Risco', layout='wide')
	st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
	clips_page = st.Page(show_clips, title='Clipes', icon='🎬', url_path='clips')
	alerts_page = st.Page(lambda: show_alerts(), title='Alertas S3', icon='🚨', url_path='alerts')
	# Cameras are not operating yet. Never connect automatically in mock mode.
	use_kafka = st.sidebar.toggle('Eventos reais S2 (Kafka)', value=False,
		help='Desativado por padrão. Requer broker e componentes S2/S3 operacionais.')
	engine = None
	if not use_kafka:
		stop_kafka_engine()
	if use_kafka:
		brokers = get_kafka_bootstrap_servers()
		if not brokers:
			stop_kafka_engine()
			st.sidebar.warning('Configure A3_KAFKA_BOOTSTRAP_SERVERS; mantendo os mocks.')
		else:
			wants_alerts = os.getenv('A3_PUBLISH_S3_ALERTS', '0').strip() == '1'
			exclusive = os.getenv('A3_S3_COMMANDS_EXCLUSIVE', '0').strip() == '1'
			publish_alerts = wants_alerts and exclusive
			if wants_alerts and not exclusive:
				st.sidebar.warning('Envio S3 bloqueado. Confirme A3_S3_COMMANDS_EXCLUSIVE=1 depois de desabilitar o ROUTING automático equivalente no S3.')
			try:
				engine = get_kafka_engine(brokers, publish_alerts)
				exception = engine.last_error
				if exception:
					st.sidebar.error(f'Erro Kafka: {exception}')
				st.sidebar.caption('S3 command habilitado' if publish_alerts else 'S3 command desabilitado (seguro)')
				st.sidebar.caption(f'Broker configurado: {brokers}')
				st.sidebar.button('Atualizar eventos Kafka', key='a3_refresh_kafka')
			except Exception as exc:
				stop_kafka_engine()
				st.sidebar.error(f'Não foi possível iniciar o consumidor Kafka: {exc}')
				engine = None

	def show_simulator():
		render_header()
		render_simulator_page(clips_page=clips_page, alerts_page=alerts_page)

	def show_dashboard():
		render_header()
		if engine is not None:
			render_dashboard_page(engine.snapshot_violations(), clips_page=clips_page,
				mock_mode=False, matrix_audit=engine.snapshot_decisions(), alerts_page=alerts_page)
		else:
			sim = st.session_state.get('a3_integrated_simulator')
			if sim is not None:
				view = st.radio('Fonte do dashboard', ['Mocks fixos', 'Simulador integrado'],
					key='a3_dashboard_source', horizontal=True)
				if view == 'Simulador integrado':
					render_dashboard_page(sim.engine.snapshot_violations(), clips_page=clips_page,
						mock_mode=False, matrix_audit=sim.engine.snapshot_decisions(),
						alerts_page=alerts_page, clips_mock_mode=True,
						source_description='Eventos simulados processados pela matriz A3; histórico temporário isolado.')
				else:
					render_dashboard_page(get_mock_violations(), clips_page=clips_page, mock_mode=True, alerts_page=alerts_page)
			else:
				render_dashboard_page(get_mock_violations(), clips_page=clips_page, mock_mode=True, alerts_page=alerts_page)

	def show_alerts():
		render_header()
		render_alerts_page(engine=engine, mock_mode=engine is None, clips_page=clips_page)

	navigation = st.navigation([
		st.Page(show_dashboard, title='Dashboard', icon='📊', default=True),
		st.Page(show_zone_editor, title='Editor de zonas', icon='🗺️'),
		st.Page(show_risk_matrix, title='Matriz de decisão', icon='🛡️'),
		st.Page(show_simulator, title='Simulador integrado', icon='▶️'),
		st.Page(show_contracts, title='RULES / ROUTING (opcional)', icon='📋'),
		st.Page(render_connectivity_page, title='Teste de conexão Kafka', icon='🌐'),
		alerts_page,
		clips_page,
	])
	navigation.run()


if __name__ == '__main__':
	main()
