"""A3 configuration UI: zone semantics and deterministic, auditable decisions.

All configuration remains owned by A3; risk never becomes a field of S1 ZONES.
"""
from copy import deepcopy

import pandas as pd
import streamlit as st

from services.risk_policy import (
	LEVELS, EVENT_TYPES, ACTIONS, load_policy, save_policy, set_zone_risk,
	validate_policy, policy_for_saved_zones,
)
from services.zones import load_zones
from services.mock_validation import get_mock_matrix_evaluation


def render_risk_matrix_page():
	st.title('Matriz de decisão e classificação de risco · A3')
	st.caption('Política A3 determinística, versionada e auditável. S1 recebe apenas geometria; S2 qualifica eventos; A3 classifica risco; S3 trata alertas.')
	try:
		policy = load_policy()
		zones = load_zones()
	except (ValueError, OSError) as exc:
		st.error(f'Falha ao carregar a política ou zonas: {exc}')
		return
	st.info('A matriz inicial é uma proposta de DEMONSTRAÇÃO, não uma política oficial aprovada. Revise as regras antes do uso operacional.')
	st.markdown('**Zonas registradas no editor B6**')
	zone_rows = policy_for_saved_zones(policy, zones)
	if zone_rows:
		st.dataframe(pd.DataFrame(zone_rows), hide_index=True, use_container_width=True)
	else:
		st.warning('Nenhuma zona real registrada. Desenhe e confirme as zonas no editor B6 antes de classificá-las.')
	with st.form('a3_policy_editor'):
		st.subheader('Parâmetros gerais da política')
		c1, c2, c3 = st.columns(3)
		with c1:
			version = st.text_input('Versão A3 (altere a cada revisão)', value=policy['version'])
		with c2:
			window = st.number_input('Janela de reincidência (h)', min_value=1, max_value=720,
				value=policy['recurrence_window_hours'], step=1)
		with c3:
			escalation_after = st.number_input('Escalonar após N ocorrências anteriores',
				min_value=1, max_value=100, value=policy['recurrence_escalation_after'], step=1)
		minimum = st.slider('Confiança mínima adicional do A3 para decisão automática',
			min_value=0.0, max_value=1.0, value=float(policy.get('min_decision_confidence', 0.0)), step=0.05)
		st.subheader('Semântica de risco das zonas (exclusiva do A3)')
		choices = {}
		for zone in zones:
			current = policy['zones'].get(zone['zone_id'], {}).get('risk_level')
			options = ['UNCONFIGURED', *LEVELS]
			choices[zone['zone_id']] = st.selectbox(f"{zone['name']} ({zone['zone_id']})",
				options, index=options.index(current) if current in options else 0,
				key=f"a3_policy_zone_{zone['zone_id']}")
		st.subheader('Regras explícitas da matriz A3')
		st.caption('Precedência: classe e movimento específicos vencem *; empates ambíguos são rejeitados. Regras para saída/observação podem ter grau 0.')
		rows = deepcopy(policy['rules'])
		edited = st.data_editor(pd.DataFrame(rows), num_rows='dynamic', hide_index=True,
			use_container_width=True, key='a3_edit_matrix_rules',
			column_config={
				'risk_level': st.column_config.SelectboxColumn('risk_level', options=list(LEVELS), required=True),
				'event_type': st.column_config.SelectboxColumn('event_type', options=list(EVENT_TYPES), required=True),
				'grade': st.column_config.NumberColumn('grade', min_value=0, max_value=3, step=1, required=True),
				'action': st.column_config.SelectboxColumn('action', options=list(ACTIONS), required=True),
				'movement': st.column_config.SelectboxColumn('movement',
					options=['*', 'ENTER', 'EXIT', 'MOVING', 'STATIONARY', 'UNKNOWN'], required=True),
			})
		submit = st.form_submit_button('Validar e salvar política A3', type='primary')
	if submit:
		try:
			candidate = deepcopy(policy)
			candidate['version'] = version.strip()
			candidate['recurrence_window_hours'] = int(window)
			candidate['recurrence_escalation_after'] = int(escalation_after)
			candidate['min_decision_confidence'] = float(minimum)
			for zone_id, value in choices.items():
				if value == 'UNCONFIGURED':
					candidate['zones'].pop(zone_id, None)
				else:
					candidate = set_zone_risk(zone_id, value, policy=candidate)
			candidate['rules'] = edited.to_dict(orient='records')
			for row in candidate['rules']:
				grade = row.get('grade')
				if pd.isna(grade) or int(grade) != grade:
					raise ValueError('A grade must be an integer 0..3')
				row['grade'] = int(grade)
			validate_policy(candidate)
			path = save_policy(candidate)
			st.success(f'Política salva em {path}. As próximas decisões S2 usarão esta versão.')
			st.rerun()
		except (ValueError, TypeError, OSError, KeyError) as exc:
			st.error(f'Política inválida: {exc}')
	st.divider()
	with st.expander('Ver política completa e agenda de risco', expanded=False):
		st.caption('A função de decisão respeita schedules versionados no JSON da política. Não são misturados ao artefato ZONES do S1.')
		st.json(policy)
	st.subheader('Validação independente com os 5 cenários fictícios')
	st.caption('São usados níveis de risco e regras de DEMONSTRAÇÃO; esses resultados não modificam a política da instalação.')
	st.dataframe(get_mock_matrix_evaluation(), hide_index=True, use_container_width=True)
