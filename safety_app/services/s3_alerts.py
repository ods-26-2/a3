"""A3's S3 client and durable projections (S3 remains owner of alert history).

Real mode: command/query via contract 6c, events subscribed on ods.s3.alert.
Mock mode: in-process S3 contract simulator, with a *separate* SQLite store.
The local projection is a cache for operator UI, not an S3 replacement.
"""
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4, uuid5

from services.kafka_rpc import request_reply

ALERT_COMMAND = 'ods.s3.alert.command'
ALERT_ACTION = 'ods.s3.alert_action.command'
ALERT_EVENT = 'ods.s3.alert'
ALERT_QUERY = 'ods.s3.alerts.query'
ALERT_STATES = ('CREATED', 'ACKNOWLEDGED', 'MUTED', 'CLOSED')
SEVERITIES = ('LOW', 'MEDIUM', 'HIGH')
CHANNELS = ('SCREEN', 'SOUND', 'PUSH')
DEFAULT_STORE = Path(__file__).resolve().parents[1] / 'data' / 's3_alert_projection.sqlite3'
DEFAULT_MOCK_STORE = Path(__file__).resolve().parents[1] / 'data' / 'mock_s3_alerts.sqlite3'


def utc_now():
	return datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def valid_uuid(value):
	try:
		UUID(str(value))
		return True
	except (ValueError, AttributeError, TypeError):
		return False


def _utc(value):
	if not isinstance(value, str):
		raise ValueError('Expected a UTC ISO datetime')
	try:
		date = datetime.fromisoformat(value.replace('Z', '+00:00'))
	except (ValueError, TypeError) as exc:
		raise ValueError('Invalid ISO datetime') from exc
	if date.tzinfo is None or date.utcoffset().total_seconds() != 0:
		raise ValueError('Datetime must be UTC')
	return date


def validate_s3_alert(payload):
	if not isinstance(payload, dict) or not valid_uuid(payload.get('alert_id')):
		raise ValueError('S3 alert requires a UUID alert_id')
	if payload.get('event_id') is not None and not valid_uuid(payload['event_id']):
		raise ValueError('S3 event_id must be UUID')
	if not all(isinstance(payload.get(key), str) and payload[key].strip() for key in ('title', 'message')):
		raise ValueError('S3 alert requires title and message')
	if payload.get('severity') not in SEVERITIES or payload.get('state') not in ALERT_STATES:
		raise ValueError('S3 alert severity/state is invalid')
	for key in ('recipients', 'channels'):
		if not isinstance(payload.get(key), list) or not payload[key] or not all(isinstance(s, str) and s for s in payload[key]):
			raise ValueError(f'S3 alert {key} must be a nonempty array')
	if any(channel not in CHANNELS for channel in payload['channels']):
		raise ValueError('Unknown S3 alert channel')
	if any(not any(s.startswith(prefix) and len(s) > len(prefix) for prefix in ('user:', 'role:', 'group:'))
		for s in payload['recipients']):
		raise ValueError('Invalid S3 recipient prefix')
	if payload.get('clip_id') is not None and not valid_uuid(payload['clip_id']):
		raise ValueError('Invalid S3 clip_id')
	_utc(payload.get('created_at'))
	for key in ('acknowledged_at', 'muted_until', 'closed_at'):
		if payload.get(key) is not None:
			_utc(payload[key])
	return payload


def build_alerts_query(from_at=None, to_at=None, state=None, severity=None, limit=100):
	if type(limit) is not int or not 1 <= limit <= 500:
		raise ValueError('S3 history limit must be integer 1..500')
	filters = {'limit': limit}
	for key, value in [('from_at', from_at), ('to_at', to_at)]:
		if value:
			_utc(value)
			filters[key] = value
	if from_at and to_at and _utc(from_at) > _utc(to_at):
		raise ValueError('from_at must not be later than to_at')
	if state is not None:
		if state not in ALERT_STATES:
			raise ValueError('Invalid S3 alert state filter')
		filters['state'] = state
	if severity is not None:
		if severity not in SEVERITIES:
			raise ValueError('Invalid S3 severity filter')
		filters['severity'] = severity
	return {'message_type': 'query', 'message_id': str(uuid4()), 'schema': ALERT_QUERY,
		'schema_version': '1.0', 'producer': 'A3', 'target': 'S3',
		'published_at': utc_now(), 'payload': filters}


def build_alert_action_command(alert_id, action, actor, mute_until=None, reason=None):
	"""Construct an A3→S3 VER-3 action; no local change to S3 alert state."""
	if not valid_uuid(alert_id):
		raise ValueError('S3 action requires UUID alert_id')
	if action not in ('ACKNOWLEDGE', 'MUTE', 'CLOSE'):
		raise ValueError('Unsupported S3 action')
	if not isinstance(actor, str) or not actor.startswith('user:') or not actor[5:].strip():
		raise ValueError('S3 action actor must use user:<id>')
	payload = {'alert_id': alert_id, 'action': action, 'actor': actor}
	if action == 'MUTE':
		if _utc(mute_until) <= datetime.now(timezone.utc):
			raise ValueError('mute_until must be in the future')
		payload['mute_until'] = mute_until
	elif mute_until is not None:
		raise ValueError('mute_until is only supported for MUTE')
	if reason is not None:
		if not isinstance(reason, str) or not reason.strip():
			raise ValueError('reason must be nonempty when provided')
		payload['reason'] = reason.strip()
	return {'schema': ALERT_ACTION, 'schema_version': '1.0', 'message_type': 'command',
		'message_id': str(uuid4()), 'producer': 'A3', 'target': 'S3',
		'published_at': utc_now(), 'payload': payload}


def _mock_envelope(alert, when, sequence=1):
	"""Complete 6c EV-SERVIÇO fixture emitted by the simulated S3."""
	return {'schema': ALERT_EVENT, 'schema_version': '1.0', 'message_type': 'event',
		'message_id': str(uuid4()), 'producer': 'S3',
		'published_at': when, 'captured_at': when,
		'session_id': str(uuid5(UUID(alert['alert_id']), 'mock-s3-session')),
		'sequence': sequence, 'correlation_id': alert.get('event_id'),
		'payload': alert}


class S3AlertStore:
	"""Only stores S3-issued alerts and the A3 attempts that requested them."""
	def __init__(self, path=None, mock=False):
		default = DEFAULT_MOCK_STORE if mock else DEFAULT_STORE
		self.path = str(Path(path or os.getenv('A3_MOCK_S3_STORE_PATH' if mock else 'A3_S3_PROJECTION_PATH', str(default))).expanduser().resolve())
		Path(self.path).parent.mkdir(parents=True, exist_ok=True)
		with self._conn() as db:
			db.execute('''CREATE TABLE IF NOT EXISTS a3_s3_requests (
				message_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, grade INTEGER NOT NULL,
				status TEXT NOT NULL, alert_id TEXT, error TEXT,
				command_json TEXT NOT NULL, reply_json TEXT, updated_at TEXT NOT NULL,
				UNIQUE(event_id, grade))''')
			db.execute('''CREATE TABLE IF NOT EXISTS a3_s3_alerts (
				alert_id TEXT PRIMARY KEY, event_id TEXT, severity TEXT NOT NULL,
				state TEXT NOT NULL, created_at TEXT NOT NULL, published_at TEXT NOT NULL,
				payload_json TEXT NOT NULL)''')
			db.execute('CREATE INDEX IF NOT EXISTS idx_a3_s3_alert_event ON a3_s3_alerts(event_id)')
			db.execute('CREATE INDEX IF NOT EXISTS idx_a3_s3_alert_created ON a3_s3_alerts(created_at)')

	@contextmanager
	def _conn(self):
		conn = sqlite3.connect(self.path, timeout=5)
		conn.execute('PRAGMA busy_timeout=5000')
		try:
			with conn:
				yield conn
		finally:
			conn.close()

	def record_attempt(self, command, grade):
		"""False means a grade was already attempted; never resend automatically."""
		if command.get('schema') != ALERT_COMMAND or not valid_uuid(command.get('message_id')):
			raise ValueError('Expected A3 S3 command')
		payload = command.get('payload', {})
		if not valid_uuid(payload.get('event_id')) or grade not in (1, 2, 3):
			raise ValueError('S3 attempt requires a valid event_id and grade')
		with self._conn() as db:
			cursor = db.execute('''INSERT OR IGNORE INTO a3_s3_requests
				(message_id, event_id, grade, status, command_json, updated_at)
				VALUES (?,?,?,?,?,?)''', (command['message_id'], payload['event_id'], grade,
				'PENDING', json.dumps(command, ensure_ascii=False), utc_now()))
			return cursor.rowcount == 1

	def record_reply(self, message_id, reply=None, error=None):
		"""Timeout is UNKNOWN: never treat broker send as S3 confirmation."""
		if error is not None:
			# Even an I/O error may occur after S3 accepted the command.
			status = 'UNKNOWN'
			alert_id = None
			description = str(error)
		else:
			reply = reply if isinstance(reply, dict) else {}
			alert_id = reply.get('alert_id')
			status = 'CONFIRMED' if reply.get('status') == 'OK' and valid_uuid(alert_id) else 'REJECTED'
			description = None if status == 'CONFIRMED' else json.dumps(reply, ensure_ascii=False)
		with self._conn() as db:
			old = db.execute('SELECT status FROM a3_s3_requests WHERE message_id=?', (message_id,)).fetchone()
			if old is None:
				raise ValueError('S3 reply has no corresponding A3 request')
			if old[0] == 'CONFIRMED':
				return 'CONFIRMED'  # A later timeout cannot undo a received S3 event.
			db.execute('''UPDATE a3_s3_requests SET status=?, alert_id=COALESCE(?,alert_id),
				error=?, reply_json=?, updated_at=? WHERE message_id=?''',
				(status, alert_id, description,
				 json.dumps(reply, ensure_ascii=False) if reply is not None else None,
				 utc_now(), message_id))
		return status

	def record_event(self, envelope):
		if not isinstance(envelope, dict) or envelope.get('schema') != ALERT_EVENT or envelope.get('message_type') != 'event' or envelope.get('producer') != 'S3':
			raise ValueError('Expected ods.s3.alert event from S3')
		# 6c EV-SERVIÇO profile. No fabricated missing fields from real messages.
		if not valid_uuid(envelope.get('message_id')) or envelope.get('schema_version') != '1.0':
			raise ValueError('S3 envelope requires message_id and schema_version=1.0')
		if not valid_uuid(envelope.get('session_id')) or type(envelope.get('sequence')) is not int or envelope['sequence'] < 0:
			raise ValueError('S3 service event requires session_id and sequence')
		_utc(envelope.get('captured_at'))
		if envelope.get('correlation_id') is not None and not valid_uuid(envelope['correlation_id']):
			raise ValueError('Invalid S3 correlation_id')
		payload = dict(validate_s3_alert(envelope.get('payload')))
		published = envelope.get('published_at')
		_utc(published)
		with self._conn() as db:
			old = db.execute('SELECT published_at FROM a3_s3_alerts WHERE alert_id=?', (payload['alert_id'],)).fetchone()
			if old and _utc(old[0]) >= _utc(published):
				return False
			# Associate with the initiating A3 command when S3 omits event_id.
			if not payload.get('event_id') and valid_uuid(envelope.get('correlation_id')):
				row = db.execute('SELECT event_id FROM a3_s3_requests WHERE message_id=?',
					(envelope['correlation_id'],)).fetchone()
				if row:
					payload['event_id'] = row[0]
			db.execute('''INSERT INTO a3_s3_alerts
				(alert_id,event_id,severity,state,created_at,published_at,payload_json)
				VALUES (?,?,?,?,?,?,?) ON CONFLICT(alert_id) DO UPDATE SET
				event_id=COALESCE(excluded.event_id,a3_s3_alerts.event_id),
				severity=excluded.severity, state=excluded.state,
				created_at=excluded.created_at,published_at=excluded.published_at,
				payload_json=excluded.payload_json''',
				(payload['alert_id'], payload.get('event_id'), payload['severity'],
				 payload['state'], payload['created_at'], published, json.dumps(payload, ensure_ascii=False)))
			# Only command message_id proves this was OUR request. S3 may also
			# create an automatic ROUTING alert for the same S2 event_id.
			db.execute('''UPDATE a3_s3_requests SET status='CONFIRMED', alert_id=?, error=NULL,
				updated_at=? WHERE message_id=?''',
				(payload['alert_id'], utc_now(), envelope.get('correlation_id')))
		return True

	def ingest_query_reply(self, reply):
		if not isinstance(reply, dict) or reply.get('status') != 'OK' or not isinstance(reply.get('alerts'), list):
			raise RuntimeError(f'Invalid S3 alerts.query reply: {reply}')
		count = 0
		for index, payload in enumerate(reply['alerts']):
			# A query is an authoritative S3 snapshot; record its receive time
			# rather than treating the alert creation timestamp as last update.
			when = utc_now()
			if self.record_event({'schema': ALERT_EVENT, 'schema_version': '1.0',
				'message_type': 'event', 'message_id': str(uuid4()), 'producer': 'S3',
				'session_id': str(uuid5(UUID(payload['alert_id']), 'a3-query-snapshot')),
				'sequence': index, 'captured_at': when, 'published_at': when,
				'payload': payload}):
				count += 1
		return count

	def list_alerts(self, event_id=None, state=None, severity=None, from_at=None, to_at=None, limit=100):
		if type(limit) is not int or not 1 <= limit <= 500:
			raise ValueError('S3 history limit must be 1..500')
		conditions, args = [], []
		for name, value in [('event_id', event_id), ('state', state), ('severity', severity)]:
			if value:
				conditions.append(f'{name}=?')
				args.append(value)
		for op, value in [('>=', from_at), ('<=', to_at)]:
			if value:
				_utc(value)
				conditions.append(f'julianday(created_at){op}julianday(?)')
				args.append(value)
		where = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
		with self._conn() as db:
			rows = db.execute('SELECT payload_json FROM a3_s3_alerts' + where +
				' ORDER BY julianday(created_at) DESC, alert_id DESC LIMIT ?', (*args, limit)).fetchall()
		return [json.loads(row[0]) for row in rows]

	def list_requests(self, event_id=None, limit=100):
		if type(limit) is not int or not 1 <= limit <= 500:
			raise ValueError('Request limit must be 1..500')
		with self._conn() as db:
			if event_id:
				rows = db.execute('''SELECT message_id,event_id,grade,status,alert_id,error,updated_at
					FROM a3_s3_requests WHERE event_id=? ORDER BY updated_at DESC LIMIT ?''',
					(event_id, limit)).fetchall()
			else:
				rows = db.execute('''SELECT message_id,event_id,grade,status,alert_id,error,updated_at
					FROM a3_s3_requests ORDER BY updated_at DESC LIMIT ?''', (limit,)).fetchall()
		return [dict(zip(('message_id', 'event_id', 'grade', 'status', 'alert_id', 'error', 'updated_at'), row)) for row in rows]


class S3Client:
	"""Real S3 contract client, with query reconciliation into A3's local cache."""
	def __init__(self, brokers, store=None, timeout=None):
		self.brokers = brokers
		self.store = store or S3AlertStore()
		self.timeout = float(timeout or os.getenv('A3_RPC_TIMEOUT_SEC', '8'))

	def action(self, command):
		"""S3 executes actions. A3 awaits the correlated reply; the S3 event updates projection."""
		if command.get('schema') != ALERT_ACTION or command.get('target') != 'S3':
			raise ValueError('Expected S3 alert_action command')
		if not self.brokers:
			raise RuntimeError('S3 action requires Kafka brokers')
		reply = request_reply(command, ALERT_ACTION, brokers=self.brokers, timeout=self.timeout)
		if not isinstance(reply, dict) or reply.get('status') != 'OK':
			raise RuntimeError(f'S3 rejected action: {reply}')
		expected = {'ACKNOWLEDGE': 'ACKNOWLEDGED', 'MUTE': 'MUTED',
			'CLOSE': 'CLOSED'}.get(command['payload'].get('action'))
		if reply.get('alert_id') != command['payload']['alert_id'] or reply.get('state') != expected:
			raise RuntimeError('S3 action reply contains inconsistent alert_id/state')
		return reply

	def history(self, from_at=None, to_at=None, state=None, severity=None, limit=100):
		if not self.brokers:
			raise RuntimeError('S3 history query requires Kafka brokers')
		command = build_alerts_query(from_at=from_at, to_at=to_at, state=state,
			severity=severity, limit=limit)
		reply = request_reply(command, ALERT_QUERY, brokers=self.brokers, timeout=self.timeout)
		self.store.ingest_query_reply(reply)
		return {'alerts': reply['alerts'], 'next_from_at': reply.get('next_from_at')}

	def history_by_event(self, event_id, limit=500, max_pages=10):
		"""Look up one event by paginating the official S3 query contract.

		S3 has no event_id query parameter. Filtering is done in A3 ONLY;
		complete=False makes the bounded search limitation explicit.
		"""
		if not valid_uuid(event_id):
			raise ValueError('S3 history_by_event requires UUID event_id')
		if type(max_pages) is not int or not 1 <= max_pages <= 30:
			raise ValueError('max_pages must be integer 1..30')
		results, cursor, cursors = {}, None, set()
		for page in range(1, max_pages + 1):
			response = self.history(from_at=cursor, limit=limit)
			for alert in response['alerts']:
				if alert.get('event_id') == event_id:
					results[alert['alert_id']] = alert
			new_cursor = response.get('next_from_at')
			if not new_cursor:
				return {'alerts': list(results.values()), 'pages_checked': page,
					'complete': True, 'next_from_at': None}
			if new_cursor in cursors:
				return {'alerts': list(results.values()), 'pages_checked': page,
					'complete': False, 'next_from_at': new_cursor}
			cursors.add(new_cursor)
			cursor = new_cursor
		return {'alerts': list(results.values()), 'pages_checked': max_pages,
			'complete': False, 'next_from_at': cursor}


class MockS3Client:
	"""Deterministic offline S3 *simulator*. Its data never reaches live S3."""
	def __init__(self, store=None):
		self.store = store or S3AlertStore(mock=True)

	def submit(self, command, grade):
		if command.get('schema') != ALERT_COMMAND or command.get('target') != 'S3':
			raise ValueError('Invalid S3 alert command')
		if not self.store.record_attempt(command, grade):
			return {'status': 'DUPLICATE', 'message_id': command['message_id']}
		payload = command['payload']
		alert_id = str(uuid5(UUID(command['message_id']), 'mock-S3-alert'))
		created = utc_now()
		alert = {**payload, 'alert_id': alert_id, 'state': 'CREATED', 'created_at': created}
		self.store.record_event({**_mock_envelope(alert, created, sequence=1),
			'correlation_id': command['message_id']})
		reply = {'status': 'OK', 'alert_id': alert_id}
		self.store.record_reply(command['message_id'], reply=reply)
		return reply

	def action(self, command):
		"""A mock S3 service handles the same action contract without touching real S3."""
		if command.get('schema') != ALERT_ACTION or command.get('target') != 'S3':
			raise ValueError('Expected S3 action contract')
		p = command.get('payload', {})
		build_alert_action_command(p.get('alert_id'), p.get('action'), p.get('actor'),
			mute_until=p.get('mute_until'), reason=p.get('reason'))
		state = {'ACKNOWLEDGE': 'ACKNOWLEDGED', 'MUTE': 'MUTED', 'CLOSE': 'CLOSED'}[p['action']]
		alerts = self.store.list_alerts(limit=500)
		alert = next((row for row in alerts if row['alert_id'] == p['alert_id']), None)
		if alert is None:
			return {'status': 'ERROR', 'error': {'code':'ALERT_NOT_FOUND'}}
		if alert['state'] == 'CLOSED' or alert['state'] == state:
			return {'status': 'ERROR', 'error': {'code':'INVALID_TRANSITION'}}
		when = utc_now()
		updated = {**alert, 'state': state}
		if state == 'ACKNOWLEDGED':
			updated.update(acknowledged_at=when, acknowledged_by=p['actor'])
		if state == 'MUTED':
			updated.update(muted_until=p['mute_until'])
		if state == 'CLOSED':
			updated.update(closed_at=when, closed_by=p['actor'])
		self.store.record_event(_mock_envelope(updated, when, sequence=2))
		return {'status':'OK', 'alert_id':p['alert_id'], 'state':state}

	def update_mock_state(self, alert_id, state):
		if state not in ALERT_STATES:
			raise ValueError('Unknown S3 state')
		alerts = self.store.list_alerts(limit=500)
		payload = next((a for a in alerts if a['alert_id'] == alert_id), None)
		if payload is None:
			raise ValueError('Mock alert not found')
		if payload['state'] == 'CLOSED' and state != 'CLOSED':
			raise ValueError('A closed alert cannot be reopened in the simulation')
		when = utc_now()
		updated = {**payload, 'state': state}
		if state == 'ACKNOWLEDGED':
			updated.update(acknowledged_at=when, acknowledged_by='user:mock-operator')
		if state == 'CLOSED':
			updated.update(closed_at=when, closed_by='user:mock-operator')
		self.store.record_event(_mock_envelope(updated, when, sequence=2))
		return updated
