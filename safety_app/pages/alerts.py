"""Operator-facing A3 alert routing, S3 states and S3 history (real or mock)."""
import json
import os
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st

from components.b6_alert_console import render_b6_alert_console
from services.alert_delivery import CHANNELS, load_delivery_policy, save_delivery_policy
from services.kafka_engine import build_s3_alert_command
from services.mock_validation import get_mock_decision, get_scenarios
from services.s3_alerts import (
	ALERT_STATES, SEVERITIES, S3AlertStore, S3Client, MockS3Client, build_alert_action_command,
	valid_uuid,
)


def _render_route_editor():
	with st.expander('Destinatários e canais por grau da matriz', expanded=False):
		st.caption('Configuração exclusiva dos comandos A3→S3. Não publica artefato ROUTING automático no S3.')
		try:
			policy = load_delivery_policy()
		except (OSError, ValueError, json.JSONDecodeError) as exc:
			st.error(f'Política de entrega inválida: {exc}')
			return
		st.warning('Destinatários atuais são valores de demonstração. Configure identificadores válidos antes de usar envio real.')
		with st.form('a3_delivery_editor'):
			version = st.text_input('Versão da configuração de entrega', value=policy['version'])
			routes = []
			for grade in (1, 2, 3):
				current = next(r for r in policy['routes'] if r['grade'] == grade)
				st.markdown(f'**Grau {grade} — {("LOW", "MEDIUM", "HIGH")[grade-1]}**')
				recipients = st.text_input(f'Destinatários do grau {grade} (separados por vírgula)',
					value=', '.join(current['recipients']), key=f'a3_recipients_{grade}')
				channels = st.multiselect(f'Canais do grau {grade}', options=list(CHANNELS),
					default=current['channels'], key=f'a3_channels_{grade}')
				routes.append({'grade': grade,
					'recipients': [v.strip() for v in recipients.split(',') if v.strip()],
					'channels': channels})
			submit = st.form_submit_button('Validar e salvar destinatários/canais', type='primary')
		if submit:
			try:
				location = save_delivery_policy({'version': version.strip(), 'routes': routes})
				st.success(f'Política registrada em {location}. Será utilizada nas próximas decisões.')
				st.rerun()
			except (ValueError, OSError) as exc:
				st.error(str(exc))


def _display_rows(alerts):
	return pd.DataFrame([{
		'alert_id': alert.get('alert_id'), 'event_id': alert.get('event_id'),
		'Gravidade': alert.get('severity'), 'Estado S3': alert.get('state'),
		'Título': alert.get('title'), 'Criado em': alert.get('created_at'),
		'Destinatários': ', '.join(alert.get('recipients') or []),
		'Canais': ', '.join(alert.get('channels') or []),
	} for alert in alerts])


def _handle_b6_action(result, alerts, store, mock_mode, engine, clips_page, operator):
	"""Delegate B6 UI callbacks to S3's public contract; never mutate S3 state in A3."""
	if not isinstance(result, dict) or not result.get('request_id'):
		return
	if st.session_state.get('a3_last_b6_alert_action') == result['request_id']:
		return
	# Deduplicate before remote I/O: if S3 times out, do not silently retry.
	st.session_state['a3_last_b6_alert_action'] = result['request_id']
	kind = result.get('kind')
	alert_id = result.get('alert_id')
	by_id = {alert['alert_id']: alert for alert in alerts}
	if kind == 'FOCUS':
		alert = by_id.get(alert_id)
		if alert is None or not alert.get('event_id'):
			st.warning('Este alerta não está associado a um event_id do S2.')
			return
		if clips_page is None:
			st.warning('A navegação para o player não está disponível nesta página.')
			return
		st.session_state['a3_clip_event_id'] = alert['event_id']
		st.session_state['a3_mock_clips_enabled'] = mock_mode
		st.session_state['a3_clip_auto_lookup'] = True
		st.session_state.pop('a3_s4_clips', None)
		st.switch_page(clips_page)
		return
	if kind not in ('ACKNOWLEDGE', 'MUTE', 'CLOSE', 'ACKNOWLEDGE_ALL'):
		st.error('Ação desconhecida no console B6.3.')
		return
	if not operator.startswith('user:') or not operator[5:].strip():
		st.error('Configure o identificador do operador (user:<id>) antes de executar ações no S3.')
		return
	selected = ([row for row in alerts if row['state'] == 'CREATED'][:20]
		if kind == 'ACKNOWLEDGE_ALL' else [by_id[alert_id]] if alert_id in by_id else [])
	if not selected:
		st.warning('Nenhum alerta disponível para esta ação.')
		return
	client = MockS3Client(store=store) if mock_mode else S3Client(brokers=engine.brokers, store=store)
	sent, failures = 0, []
	for alert in selected:
		try:
			action = 'ACKNOWLEDGE' if kind == 'ACKNOWLEDGE_ALL' else kind
			mute_until = ((datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat()
				if action == 'MUTE' else None)
			command = build_alert_action_command(alert['alert_id'], action, operator, mute_until=mute_until)
			reply = client.action(command)
			if reply.get('status') != 'OK':
				failures.append(f"{alert['alert_id']}: {reply.get('error', reply)}")
			else:
				sent += 1
		except (ValueError, RuntimeError, TimeoutError, OSError) as exc:
			failures.append(f"{alert['alert_id']}: {exc}")
	if sent:
		st.success(f'{sent} ação(ões) confirmada(s) pelo S3. O estado só é atualizado após evento/resposta do S3.')
	if failures:
		st.error('; '.join(failures))
		st.caption('Falhas por timeout não são reenviadas automaticamente.')
	if sent and not failures:
		st.rerun()


def render_alerts_page(engine=None, mock_mode=True, clips_page=None):
	st.title('Alertas e encaminhamento · A3 / S3')
	st.caption('A3 decide risco, gravidade, destinatários e canais; S3 cria, entrega e mantém o histórico dos alertas.')
	_render_route_editor()
	st.divider()
	sim = st.session_state.get('a3_integrated_simulator') if mock_mode else None
	sim_source = False
	if sim is not None:
		sim_source = st.radio('Fonte dos alertas simulados',
			['Mocks independentes', 'Simulador integrado'], key='a3_alert_display_source',
			horizontal=True) == 'Simulador integrado'
	store = (sim.engine.s3_store if sim_source else
		S3AlertStore(mock=True) if mock_mode else engine.s3_store)
	if mock_mode:
		st.info('MODO MOCK: S3 simulado localmente. Nenhum comando, consulta ou alerta é publicado no Kafka.')
		mock_client = MockS3Client(store=store)
		if sim_source:
			st.caption('Nesta fonte, somente as etapas do Simulador integrado criam comandos S3.')
		if st.button('Simular envio A3 → S3 para os 3 eventos de teste', type='primary', disabled=sim_source):
			created, existing = 0, 0
			try:
				for scenario in get_scenarios():
					if scenario['case'] != 'VIOLATION':
						continue
					decision = get_mock_decision(scenario)
					if decision.action != 'ALERT':
						continue
					command = build_s3_alert_command(scenario['envelope']['payload'], decision)
					reply = mock_client.submit(command, decision.grade)
					if reply['status'] == 'OK':
						created += 1
					else:
						existing += 1
				st.success(f'{created} alerta(s) criado(s) no S3 simulado; {existing} duplicata(s) evitada(s).')
			except (ValueError, OSError, RuntimeError) as exc:
				st.error(f'Falha na simulação: {exc}')
	else:
		st.info('MODO REAL: eventos ods.s3.alert recebidos pelo Kafka e histórico consultado diretamente no S3 quando solicitado.')
		if not engine.publish_alerts:
			st.warning('Envio A3→S3 está desabilitado. Configure A3_PUBLISH_S3_ALERTS=1 para habilitá-lo após revisar duplicidade com ROUTING.')
		st.caption('Consultar não cria alertas. Histórico local é uma projeção de eventos recebidos, não o armazenamento oficial do S3.')

	with st.form('a3_alert_filters'):
		c1, c2, c3 = st.columns(3)
		with c1:
			event_id = st.text_input('ID do evento S2 (busca paginada no S3)',
				value=st.session_state.get('a3_alert_event_id', ''),
				placeholder='UUID; vazio = todos')
		with c2:
			state = st.selectbox('Estado', ['Todos', *ALERT_STATES])
		with c3:
			severity = st.selectbox('Severidade', ['Todas', *SEVERITIES])
		limit = st.number_input('Quantidade máxima por consulta (até 500)', min_value=1,
			max_value=500, value=100, step=10)
		query = st.form_submit_button('Consultar histórico do S3')
	if event_id.strip() and not valid_uuid(event_id.strip()):
		st.error('event_id deve ser UUID ou vazio.')
		return
	selected_state = state if state != 'Todos' else None
	selected_severity = severity if severity != 'Todas' else None
	if query:
		st.session_state['a3_alert_event_id'] = event_id.strip()
		if not mock_mode:
			try:
				client = S3Client(brokers=engine.brokers, store=store)
				if event_id.strip():
					data = client.history_by_event(event_id.strip(), limit=int(limit), max_pages=10)
					st.session_state['a3_s3_next_from_at'] = data['next_from_at']
					if data['alerts']:
						st.success(f'Localizados {len(data["alerts"])} alerta(s) em {data["pages_checked"]} página(s) do S3.')
					elif data['complete']:
						st.info('A consulta concluiu sem encontrar alertas associados a este event_id.')
					else:
						st.warning('Busca limitada a 10 páginas. Não encontrar alertas não significa ausência no histórico completo do S3.')
				else:
					data = client.history(state=selected_state, severity=selected_severity, limit=int(limit))
					st.session_state['a3_s3_next_from_at'] = data['next_from_at']
					st.success(f'S3 retornou {len(data["alerts"])} alerta(s) nesta página.')
				st.session_state['a3_s3_last_query_at'] = datetime.now(timezone.utc).isoformat()
			except (RuntimeError, TimeoutError, ValueError, OSError) as exc:
				st.error(f'Consulta S3 não confirmada: {exc}. Exibindo apenas projeção local.')
		else:
			st.success('Histórico consultado no S3 simulado.')
	if not mock_mode and st.session_state.get('a3_s3_next_from_at'):
		st.caption('A busca remota do S3 não aceita event_id; o A3 filtra as páginas recebidas. next_from_at é o cursor devolvido pelo S3.')
		if st.button('Buscar próxima página do histórico S3'):
			try:
				data = S3Client(brokers=engine.brokers, store=store).history(
					from_at=st.session_state['a3_s3_next_from_at'],
					state=selected_state, severity=selected_severity, limit=int(limit))
				st.session_state['a3_s3_next_from_at'] = data['next_from_at']
				st.success(f'Mais {len(data["alerts"])} alerta(s) recebidos do S3.')
			except (RuntimeError, TimeoutError, ValueError, OSError) as exc:
				st.error(str(exc))
	st.subheader('Console de Alertas — B6.3')
	st.caption('A interface de cartões, filtros e ações pertence ao B6; o A3 somente adapta os alertas S3 e encaminha callbacks. '
		'O estado oficial é publicado pelo S3. Alertas encerrados permanecem disponíveis na consulta histórica.')
	alerts = store.list_alerts(event_id=event_id.strip() or None,
		state=selected_state, severity=selected_severity, limit=int(limit))
	mode = 'mock' if mock_mode else 'real'
	if st.session_state.get('a3_s3_actor_mode') != mode:
		st.session_state['a3_s3_action_actor'] = ('user:mock-operator' if mock_mode
			else os.environ.get('A3_OPERATOR_ID', ''))
		st.session_state['a3_s3_actor_mode'] = mode
	operator = st.text_input('Identificador do operador para ações no S3',
		key='a3_s3_action_actor',
		placeholder='user:<id>', help='A3 não autentica usuários; em produção forneça um identificador validado externamente.')
	st.caption('Silenciar usa um prazo de 15 minutos. O comando será validado e executado pelo S3. '
		'No modo real, um timeout não comprova rejeição nem aceitação.')
	callback = render_b6_alert_console(alerts)
	_handle_b6_action(callback, alerts, store, mock_mode, engine, clips_page, operator.strip())
	with st.expander('Registro tabular e auditoria dos alertas S3 (inclusive encerrados)'):
		if alerts:
			st.dataframe(_display_rows(alerts), use_container_width=True, hide_index=True)
		else:
			st.info('Nenhum alerta S3 disponível para os filtros selecionados.')
	if mock_mode and alerts and not sim_source:
		with st.expander('Demonstrar mudanças de estado emitidas pelo S3 simulado'):
			st.caption('Gera um novo evento ods.s3.alert local, NÃO executa ods.s3.alert_action.command real.')
			selected = st.selectbox('Alerta simulado', [a['alert_id'] for a in alerts])
			new_state = st.selectbox('Novo estado S3', list(ALERT_STATES))
			if st.button('Simular publicação de estado S3'):
				try:
					mock_client.update_mock_state(selected, new_state)
					st.rerun()
				except ValueError as exc:
					st.error(str(exc))
	st.subheader('Comandos enviados e respostas S3')
	st.caption('CONFIRMED = S3 respondeu OK ou publicou um evento de confirmação. UNKNOWN = timeout, sem reenvio automático.')
	requests = store.list_requests(event_id=event_id.strip() or None, limit=int(limit))
	if requests:
		st.dataframe(pd.DataFrame(requests), use_container_width=True, hide_index=True)
	else:
		st.caption('Ainda não há solicitações de alerta A3 registradas nesta fonte de dados.')
