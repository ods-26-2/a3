"""A3 dashboard: local five-case demos by default, optional live S2 projection."""
import streamlit as st
import pandas as pd

from components.violations_table import render_violations_table_component
from components.violations_chart import render_violations_chart_component
from services.mock_validation import get_mock_normal_observations, get_mock_matrix_evaluation


def render_dashboard_page(df_violations: pd.DataFrame, clips_page=None, mock_mode=True,
		matrix_audit=None, alerts_page=None, clips_mock_mode=None, source_description=None):
	if source_description:
		st.info(source_description)
	elif mock_mode:
		st.info('Demonstração A3: 3 eventos qualificados S2 e 2 observações normais S1, todos simulados.')
	else:
		st.info('Eventos reais S2 consumidos no Kafka e classificados pela política A3. Histórico SQLite persistente.')
	if df_violations.empty:
		st.warning('Nenhuma violação disponível nesta fonte de dados.')
	else:
		render_violations_table_component(df_violations, clips_page=clips_page,
			mock_mode=mock_mode if clips_mock_mode is None else clips_mock_mode, alerts_page=alerts_page)
	st.divider()
	if mock_mode:
		st.subheader('Observações sem violação')
		st.caption('As mensagens S1 de ocupação normal não possuem event_id do S2 e não são tratadas como incidentes.')
		st.dataframe(get_mock_normal_observations(), hide_index=True, use_container_width=True)
		with st.expander('Auditoria: decisões efetivamente calculadas nos 3 mocks', expanded=True):
			st.caption('A política de DEMONSTRAÇÃO possui riscos definidos apenas para suas cinco zonas fictícias; não altera a política real salva no A3.')
			st.dataframe(get_mock_matrix_evaluation(), use_container_width=True, hide_index=True)
	elif matrix_audit:
		with st.expander('Auditoria de decisões S2 → A3', expanded=False):
			rows = [{'event_id': event_id,
				'grau': decision.get('grade'), 'severidade': decision.get('severity'),
				'ação': decision.get('action'), 'regra': decision.get('matched_rule'),
				'risco': decision.get('risk_level'),
				'política': decision.get('policy_version'),
				'motivos': '; '.join(decision.get('reasons', []))}
				for event_id, decision in matrix_audit.items()]
			st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
	st.divider()
	render_violations_chart_component(df_violations)
