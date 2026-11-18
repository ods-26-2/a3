"""Contract-oriented, offline end-to-end A3 event simulator.

This is NOT an alternative S1, S2, S3, or S4 implementation: it generates
contract-shaped fixtures, feeds the existing A3KafkaEngine and its decision
matrix, and supplies S3/S4 replies through injectable boundaries. No Kafka,
cameras, network operations or changes to other toolkit components.

Every simulator instance owns isolated temporary SQLite databases; it cannot
alter the live A3 event history or the standalone five-case dashboard mocks.
"""
import copy
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from services.alert_delivery import load_delivery_policy
from services.clips import QUERY_TOPIC, build_clip_query
from services.kafka_engine import A3KafkaEngine
from services.mock_validation import get_mock_clips, get_scenarios
from services.risk_policy import demo_policy
from services.s3_alerts import ALERT_COMMAND, _mock_envelope, valid_uuid


SCENARIO_LABELS = {
	'zone_entry': 'Entrada em zona proibida · S2 → A3 → S3 → S4',
	'occupancy': 'Ocupação crescente · escalonamento MEDIUM → HIGH',
	'dwell': 'Permanência excessiva · clipe indisponível',
	'recurrence': 'Reincidência · registros prévios elevam o risco',
	'uncertain': 'Evento UNCERTAIN · revisão sem alerta automático',
	'normal_occupied': 'S1: ocupação dentro da capacidade · sem incidente',
	'normal_empty': 'S1: zona vazia · sem incidente',
}


def scenario_options():
	return dict(SCENARIO_LABELS)


def _timestamp(value, seconds=0):
	value = datetime.fromisoformat(value.replace('Z', '+00:00')) + timedelta(seconds=seconds)
	return value.isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def _s2_envelope(base, step_number, phase, *, duration=None, details=None, event_id=None, started_at=None):
	"""Preserve the original S2 event identity across STARTED/UPDATED/ENDED."""
	envelope = copy.deepcopy(base)
	payload = envelope['payload']
	payload['phase'] = phase
	if event_id is not None:
		payload['event_id'] = event_id
	if started_at is not None:
		payload['started_at'] = started_at
	if duration is not None:
		payload['duration_ms'] = duration
	if details is not None:
		payload['details'] = {**payload.get('details', {}), **details}
	envelope['message_id'] = str(uuid5(UUID(payload['event_id']), f'a3-integrated-{step_number}-{phase}'))
	envelope['sequence'] = step_number
	envelope['published_at'] = _timestamp(payload['started_at'], step_number + 1)
	envelope['captured_at'] = _timestamp(payload['started_at'], step_number)
	return envelope


def _entity_envelope(base, step_number=1):
	payload = base['payload']
	return {
		'schema': 'ods.s2.entity', 'schema_version': '1.0', 'message_type': 'event',
		'message_id': str(uuid5(UUID(payload['event_id']), 'a3-integrated-entity')),
		'producer': 'S2', 'session_id': base['session_id'],
		'source_id': base['source_id'], 'sequence': step_number,
		'captured_at': payload['started_at'],
		'published_at': _timestamp(payload['started_at'], 1),
		'payload': {'entities': [{
			'entity_id': payload['entity_id'], 'class': payload.get('class', 'person'),
			'zone_ids': [payload['zone_id']], 'source_ids': list(payload['source_ids']),
			'track_refs': [],
		}]},
	}


def _steps_for_violation(key, base):
	payload = base['payload']
	start_duration = int(payload['duration_ms'])
	updated_duration = max(start_duration + 10000, start_duration * 2)
	start_details, update_details = None, None
	if key == 'occupancy':
		# A3 computes severity. Only occupancy measurements are simulated.
		start_details = {'occupancy': 3, 'max_occupancy': 2}
		update_details = {'occupancy': 5, 'max_occupancy': 2}
	if key == 'dwell':
		start_details = {'dwell_s': 45, 'max_dwell_s': 30}
		update_details = {'dwell_s': 70, 'max_dwell_s': 30}
	base_start = _s2_envelope(base, 2, 'STARTED', duration=start_duration, details=start_details)
	base_update = _s2_envelope(base, 6, 'UPDATED', duration=updated_duration, details=update_details)
	base_end = _s2_envelope(base, 8, 'ENDED', duration=updated_duration + 1000, details=update_details)
	return [
		{'kind': 'S2_ENTITY', 'title': 'S2: entidade consolidada', 'envelope': _entity_envelope(base)},
		{'kind': 'S2_EVENT', 'title': 'S2: evento STARTED', 'envelope': base_start},
		{'kind': 'S3_CREATED', 'title': 'S3: publicação de alerta criado'},
		{'kind': 'S4_READY', 'title': 'S4: evidência preparada para consulta'},
		{'kind': 'S4_QUERY', 'title': 'A3: consulta ods.s4.clip.query'},
		{'kind': 'S2_EVENT', 'title': 'S2: evento UPDATED', 'envelope': base_update},
		{'kind': 'S3_CREATED', 'title': 'S3: publicação de nova gravidade, se houver'},
		{'kind': 'S2_EVENT', 'title': 'S2: evento ENDED', 'envelope': base_end},
		{'kind': 'S3_STATE', 'title': 'S3: operador reconheceu o alerta'},
		{'kind': 'S2_EVENT', 'title': 'S2: UPDATED atrasado (deve ser ignorado)',
			'envelope': _s2_envelope(base, 10, 'UPDATED', duration=updated_duration + 5000,
				details=update_details)},
	]


def _steps_for_recurrence(base):
	"""Two previous ATTENTION incidents precede a third on the same entity."""
	steps = []
	for index in range(3):
		value = copy.deepcopy(base)
		payload = value['payload']
		uid = str(uuid5(NAMESPACE_URL, f'a3-integrated-recurrence-{index}'))
		when = _timestamp('2026-10-16T12:00:00.000Z', index * 1800)
		payload.update(event_id=uid, entity_id='entity-simulator-repeat',
			event_type='ZONE_ENTER', zone_id='a3-zona-armazenamento',
			started_at=when, duration_ms=1200, details={'transition': 'ENTER'},
			rule_id='mock-s2-zone-entry')
		value['source_id'] = 'cam1'
		value['sequence'] = index * 3 + 1
		steps.append({'kind': 'S2_EVENT', 'title': f'S2: ocorrência {index + 1} — STARTED',
			'envelope': _s2_envelope(value, index * 3 + 1, 'STARTED', duration=1200)})
		steps.append({'kind': 'S3_CREATED', 'title': f'S3: alertar somente quando a matriz determinar ({index + 1})'})
		steps.append({'kind': 'S2_EVENT', 'title': f'S2: ocorrência {index + 1} — ENDED',
			'envelope': _s2_envelope(value, index * 3 + 3, 'ENDED', duration=2200)})
	steps.append({'kind': 'S3_STATE', 'title': 'S3: atualização do alerta gerado por reincidência'})
	return steps


def build_scenario(key):
	if key not in SCENARIO_LABELS:
		raise ValueError('Unknown integrated simulation scenario')
	fixtures = get_scenarios()
	if key.startswith('normal_'):
		sample = fixtures[3] if key == 'normal_occupied' else fixtures[4]
		return [{'kind': 'S1_OBSERVATION', 'title': 'S1: observação sem evento qualificado',
			'envelope': copy.deepcopy(sample['envelope'])}]
	base_index = {'zone_entry': 0, 'occupancy': 1, 'dwell': 2, 'recurrence': 0, 'uncertain': 0}[key]
	base = copy.deepcopy(fixtures[base_index]['envelope'])
	if key == 'recurrence':
		return _steps_for_recurrence(base)
	if key == 'uncertain':
		base['payload']['status'] = 'UNCERTAIN'
	return _steps_for_violation(key, base)


class MockS4Boundary:
	"""S4 query/reply fixture; no media bytes or invented media endpoint."""
	def __init__(self):
		self.available = set()

	def make_available(self, event_id):
		if not valid_uuid(event_id):
			raise ValueError('Expected S2 UUID event_id')
		self.available.add(event_id)

	def query(self, request):
		if request.get('schema') != QUERY_TOPIC or request.get('target') != 'S4':
			raise ValueError('Expected ods.s4.clip.query addressed to S4')
		payload = request.get('payload') or {}
		if set(payload) != {'event_id'} or not valid_uuid(payload.get('event_id')):
			raise ValueError('Mock simulator supports only event_id S4 queries')
		event_id = payload['event_id']
		clips = get_mock_clips(event_id) if event_id in self.available else []
		return {'status': 'OK', 'clips': copy.deepcopy(clips)}


class IntegratedEventSimulator:
	"""One local isolated simulator run. Call advance() to play each stage."""
	def __init__(self, scenario_key='zone_entry'):
		self.scenario_key = scenario_key
		self.steps = build_scenario(scenario_key)
		self.cursor = 0
		self.trace = []
		self._temporary = tempfile.TemporaryDirectory(prefix='a3-integrated-demo-')
		base = Path(self._temporary.name)
		self.s4 = MockS4Boundary()
		self.sent_commands = {}
		self.published_alerts = set()
		self.emitted_alerts = {}
		self.start_clock = datetime.now(timezone.utc).replace(microsecond=0)
		self.event_ids = {step['envelope']['payload']['event_id'] for step in self.steps
			if step['kind'] == 'S2_EVENT'}
		self.engine = A3KafkaEngine(brokers='', publish_alerts=True,
			history_path=str(base / 'a3_events.sqlite3'), s3_store_path=str(base / 'a3_s3.sqlite3'),
			alert_sender=self._s3_receive_command, policy_provider=demo_policy,
			delivery_policy_provider=load_delivery_policy)

	@property
	def complete(self):
		return self.cursor >= len(self.steps)

	def close(self):
		self.engine.stop()
		self._temporary.cleanup()

	def owns_event(self, event_id):
		return event_id in self.event_ids

	def _time(self, index):
		return (self.start_clock + timedelta(seconds=index)).isoformat(timespec='microseconds').replace('+00:00', 'Z')

	def _s3_receive_command(self, command):
		"""Simulated S3 response; A3KafkaEngine records request and correlation."""
		if command.get('schema') != ALERT_COMMAND or command.get('message_type') != 'command' or command.get('target') != 'S3':
			return {'status': 'ERROR', 'error': {'code': 'INVALID_COMMAND'}}
		payload = command.get('payload', {})
		if (not valid_uuid(payload.get('event_id')) or payload.get('severity') not in ('LOW', 'MEDIUM', 'HIGH')
			or not payload.get('recipients') or not payload.get('channels')):
			return {'status': 'ERROR', 'error': {'code': 'INVALID_RECIPIENT'}}
		message_id = command['message_id']
		self.sent_commands[message_id] = copy.deepcopy(command)
		return {'status': 'OK', 'alert_id': str(uuid5(UUID(message_id), 'mock-S3-alert'))}

	def _s3_publish_created(self):
		published = []
		for message_id, command in list(self.sent_commands.items()):
			if message_id in self.published_alerts:
				continue
			payload = copy.deepcopy(command['payload'])
			payload.update(alert_id=str(uuid5(UUID(message_id), 'mock-S3-alert')),
				state='CREATED', created_at=self._time(2 * self.cursor + 1))
			when = self._time(2 * self.cursor + 2)
			envelope = _mock_envelope(payload, when, sequence=self.cursor + 1)
			envelope['correlation_id'] = message_id
			self.engine.process_message(envelope)
			self.published_alerts.add(message_id)
			self.emitted_alerts[payload['alert_id']] = payload
			published.append(envelope)
		return published

	def _s3_change_state(self, state='ACKNOWLEDGED'):
		published = []
		for alert_id, old in list(self.emitted_alerts.items()):
			if old['state'] == state:
				continue
			payload = copy.deepcopy(old)
			payload['state'] = state
			when = self._time(2 * self.cursor + 3)
			if state == 'ACKNOWLEDGED':
				payload.update(acknowledged_at=when, acknowledged_by='user:demo-operator')
			envelope = _mock_envelope(payload, when, sequence=self.cursor + 2)
			self.engine.process_message(envelope)
			self.emitted_alerts[alert_id] = payload
			published.append(envelope)
		return published

	def query_clips(self, event_id):
		if not self.owns_event(event_id):
			return []
		request = build_clip_query(event_id=event_id)
		return self.s4.query(request)['clips']

	def advance(self):
		if self.complete:
			return None
		step = self.steps[self.cursor]
		kind = step['kind']
		index = self.cursor
		self.cursor += 1
		record = {'step': self.cursor, 'kind': kind, 'title': step['title'],
			'event_id': None, 'grade': None, 'severity': None, 'action': None,
			'outcome': '', 'message': None}
		if kind in ('S2_EVENT', 'S2_ENTITY', 'S1_OBSERVATION'):
			envelope = copy.deepcopy(step['envelope'])
			record['message'] = envelope
			if kind == 'S2_EVENT':
				event_id = envelope['payload']['event_id']
				record['event_id'] = event_id
				before = self.engine.history.get(event_id)
				decision = self.engine.process_message(envelope)
				after = self.engine.history.get(event_id)
				if decision is not None:
					record.update(grade=decision.grade, severity=decision.severity,
						action=decision.action, rule=decision.matched_rule,
						reasons=list(decision.reasons), policy_version=decision.policy_version)
				if before == after:
					record['outcome'] = 'Mensagem ignorada (repetida ou posterior ao encerramento).'
				else:
					record['outcome'] = f"Decisão A3: {decision.severity} / {decision.action}; fase {envelope['payload']['phase']}."
					requests = self.engine.s3_store.list_requests(event_id=event_id)
					if requests:
						record['outcome'] += f' Solicitações S3: {len(requests)}.'
			elif kind == 'S2_ENTITY':
				self.engine.process_message(envelope)
				record['outcome'] = 'Contexto da entidade recebido pelo adaptador A3; nenhuma violação criada.'
			else:
				self.engine.process_message(envelope)
				record['outcome'] = 'Observação S1 normal; não há evento qualificado do S2 nem alerta A3.'
		elif kind == 'S3_CREATED':
			emitted = self._s3_publish_created()
			record['message'] = emitted
			record['outcome'] = f'{len(emitted)} evento(s) ods.s3.alert / CREATED recebido(s) pelo A3.'
		elif kind == 'S3_STATE':
			emitted = self._s3_change_state()
			record['message'] = emitted
			record['outcome'] = f'{len(emitted)} mudança(s) ACKNOWLEDGED do S3 recebida(s).'
		elif kind == 'S4_READY':
			for event_id in self.event_ids:
				self.s4.make_available(event_id)
			record['outcome'] = 'S4 simulado habilitou os descritores; mídia continua fora do Kafka.'
		elif kind == 'S4_QUERY':
			replies = {}
			for event_id in self.event_ids:
				request = build_clip_query(event_id=event_id)
				replies[event_id] = {'request': request, 'reply': self.s4.query(request)}
			record['message'] = replies
			record['outcome'] = f"Consulta S4 concluída: {sum(len(x['reply']['clips']) for x in replies.values())} clipe(s)."
		else:
			raise RuntimeError('Unsupported simulator step')
		self.trace.append(record)
		return record

	def run_all(self):
		"""Bounded by the finite, prevalidated steps; no background execution."""
		results = []
		while not self.complete:
			results.append(self.advance())
		return results

	def snapshot(self):
		return {
			'scenario': self.scenario_key,
			'cursor': self.cursor,
			'steps': len(self.steps),
			'complete': self.complete,
			'events': self.engine.snapshot_all_events(),
			'alerts': self.engine.s3_store.list_alerts(),
			'requests': self.engine.s3_store.list_requests(),
			'clips': {event_id: self.query_clips(event_id) for event_id in sorted(self.event_ids)},
			'trace': list(self.trace),
		}
