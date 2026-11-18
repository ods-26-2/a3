"""Operator-facing, stepwise offline A3 integration simulator."""
import pandas as pd
import streamlit as st

from components.b6_video_player import render_b6_video_player
from services.integrated_simulator import IntegratedEventSimulator, scenario_options
from services.mock_validation import get_mock_video_data_uri


def _get_simulator():
	return st.session_state.get('a3_integrated_simulator')


def _replace_simulator(scenario):
	previous = _get_simulator()
	if previous is not None:
		previous.close()
	st.session_state['a3_integrated_simulator'] = IntegratedEventSimulator(scenario)
	st.session_state['a3_dashboard_source'] = 'Simulador integrado'
	st.session_state['a3_alert_display_source'] = 'Simulador integrado'


def render_simulator_page(clips_page=None, alerts_page=None):
	st.title('Simulador integrado de eventos · A3')
	st.info('Ambiente isolado: mensagens contratuais simuladas de S1/S2, decisão real da matriz A3, '
		'comando/resposta S3, atualização ods.s3.alert e consulta S4. Sem câmeras, Kafka ou mudanças nos serviços externos.')
	st.caption('A matriz utiliza a política de demonstração (zonas fictícias); destinatários/canais vêm da configuração A3. '
		'Os bancos SQLite desta execução são temporários, distintos do histórico operacional.')

	options = scenario_options()
	selected = st.selectbox('Cenário', list(options), format_func=lambda x: options[x], key='a3_simulator_scenario')
	sim = _get_simulator()
	if sim is not None and sim.scenario_key != selected:
		st.warning('O cenário selecionado é diferente da execução atual. Clique em Iniciar/reiniciar para aplicá-lo.')
	col1, col2, col3 = st.columns(3)
	with col1:
		if st.button('Iniciar / reiniciar cenário', type='primary', use_container_width=True):
			_replace_simulator(selected)
			st.rerun()
	with col2:
		if st.button('Avançar uma etapa', disabled=sim is None or sim.complete, use_container_width=True):
			sim.advance()
			st.rerun()
	with col3:
		if st.button('Executar até o fim', disabled=sim is None or sim.complete, use_container_width=True):
			sim.run_all()
			st.rerun()
	if sim is None:
		st.caption('Escolha um cenário e clique em Iniciar / reiniciar cenário.')
		return

	st.progress(sim.cursor / len(sim.steps), text=f'Etapa {sim.cursor} de {len(sim.steps)} · {options[sim.scenario_key]}')
	if not sim.complete:
		st.caption('Próxima etapa: ' + sim.steps[sim.cursor]['title'])
	else:
		st.success('Simulação concluída. As decisões, solicitações, estados e evidências abaixo foram produzidos pelas etapas executadas.')

	snap = sim.snapshot()
	if sim.engine.last_error:
		st.error('O processamento registrou um erro: ' + sim.engine.last_error)
	st.subheader('Linha do tempo contratual')
	if snap['trace']:
		st.dataframe(pd.DataFrame([{
			'Etapa': r['step'], 'Origem': r['kind'], 'Operação': r['title'],
			'Grau': r['grade'], 'Severidade': r['severity'], 'Ação': r['action'],
			'Resultado': r['outcome'],
		} for r in snap['trace']]), hide_index=True, use_container_width=True)
		with st.expander('Examinar envelopes e auditoria de cada etapa'):
			selected_step = st.selectbox('Etapa executada', range(len(snap['trace'])),
				format_func=lambda i: f"{snap['trace'][i]['step']}. {snap['trace'][i]['title']}")
			st.json(snap['trace'][selected_step])
	else:
		st.caption('Nenhuma mensagem foi processada ainda.')

	left, right = st.columns(2)
	with left:
		st.subheader('Eventos e decisões A3')
		if snap['events']:
			st.dataframe(pd.DataFrame([{
				'event_id': e['event_id'], 'Tipo': e['event_type'], 'Fase': e['phase'],
				'Zona': e['zone_id'], 'Grau máximo': e['decision']['grade'],
				'Ação final': e['decision']['action'], 'Regra': e['decision']['matched_rule'],
				'Reincidências': e['decision']['recurrence_count'],
			} for e in snap['events']]), hide_index=True, use_container_width=True)
		else:
			st.caption('Nenhum evento qualificado foi recebido do S2.')
	with right:
		st.subheader('Solicitações e alertas S3')
		if snap['requests']:
			st.dataframe(pd.DataFrame([{
				'event_id': r['event_id'], 'Grau': r['grade'], 'Solicitação': r['status'],
				'alert_id': r['alert_id'],
			} for r in snap['requests']]), hide_index=True, use_container_width=True)
		else:
			st.caption('Nenhum comando S3 foi solicitado pela matriz.')
		if snap['alerts']:
			st.dataframe(pd.DataFrame([{
				'alert_id': a['alert_id'], 'Estado': a['state'], 'Severidade': a['severity'],
				'Destinatários': ', '.join(a['recipients']), 'Canais': ', '.join(a['channels']),
			} for a in snap['alerts']]), hide_index=True, use_container_width=True)

	st.subheader('Evidências consultadas ao S4')
	if sim.event_ids:
		ids = sorted(sim.event_ids)
		selected_event_id = st.selectbox('Evento S2', ids, key='a3_integrated_clip_selection')
		clips = snap['clips'].get(selected_event_id, [])
		if clips:
			st.success(f'{len(clips)} descritor(es) S4 disponível(is) para este event_id.')
			st.dataframe(pd.DataFrame([{
				'clip_id': clip.get('clip_id'), 'Tipo': clip.get('media_type'),
				'Início': clip.get('start_at'), 'Fim': clip.get('end_at'),
			} for clip in clips]), hide_index=True, use_container_width=True)
			with st.expander('Prévia no player B6 (vídeo sintético local)'):
				render_b6_video_player(get_mock_video_data_uri())
		else:
			st.warning('Nenhum descritor disponível neste momento. O S4 pode ainda não ter publicado a evidência ou a fixture não possui clipe.')
		buttons = st.columns(2)
		with buttons[0]:
			if st.button('🎬 Abrir evento na página Clipes', use_container_width=True, disabled=clips_page is None):
				st.session_state['a3_clip_event_id'] = selected_event_id
				st.session_state['a3_mock_clips_enabled'] = True
				st.session_state['a3_clip_auto_lookup'] = True
				st.session_state.pop('a3_s4_clips', None)
				st.switch_page(clips_page)
		with buttons[1]:
			if st.button('🚨 Abrir histórico S3 simulado', use_container_width=True, disabled=alerts_page is None):
				st.session_state['a3_alert_event_id'] = selected_event_id
				st.session_state['a3_alert_display_source'] = 'Simulador integrado'
				st.switch_page(alerts_page)
	else:
		st.caption('Este cenário contém somente uma observação S1. Não existe event_id do S2 nem mídia associada.')

	with st.expander('Limites da simulação'):
		st.markdown('O S1 e o S2 são representados por **mensagens de entrada**, não por implementações de detecção ou qualificação. '
			'O S3 responde a comandos e publica estados simulados; não envia notificações reais. '
			'O S4 fornece descritores compatíveis com a consulta e o player usa um MP4 sintético local, '
			'fora do Kafka. As regras A3 e o histórico dos eventos são executados de verdade.')
