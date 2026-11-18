"""S3 operational contract, routing, request/reply, history, states and mocks.

Everything in this suite runs offline, using A3-owned copies of S3 envelopes.
"""
import json
import tempfile
import unittest
from pathlib import Path
from datetime import timedelta, datetime, timezone
from unittest.mock import patch
from uuid import uuid4

from services.alert_delivery import (
	default_delivery_policy, load_delivery_policy, resolve_delivery,
	save_delivery_policy, validate_delivery_policy,
)
from services.decision_matrix import evaluate_event
from services.kafka_engine import A3KafkaEngine, build_s3_alert_command
from services.mock_validation import get_scenarios, get_mock_decision
from services.risk_policy import demo_policy
from services.s3_alerts import (
	ALERT_EVENT, ALERT_QUERY, S3AlertStore, MockS3Client, S3Client,
	_mock_envelope, build_alerts_query,
)


EVENT = {'event_id': str(uuid4()), 'event_type': 'ZONE_ENTER', 'zone_id': 'a3-zona-restrita-1',
	'entity_id': 'entity-24', 'class': 'person', 'status': 'CONFIRMED', 'phase': 'STARTED',
	'confidence': 0.97, 'started_at': '2026-10-16T00:05:00Z', 'duration_ms': 1500,
	'details': {'transition': 'ENTER'}, 'source_ids': ['cam0']}


def s2_event(event=EVENT):
	return {'schema': 'ods.s2.event', 'message_type': 'event', 'producer': 'S2',
		'source_id': 'cam0', 'payload': event}


class DeliveryPolicyTests(unittest.TestCase):
	def setUp(self):
		self.tmp = tempfile.TemporaryDirectory()
		self.file = str(Path(self.tmp.name) / 'delivery.json')

	def tearDown(self):
		self.tmp.cleanup()

	def test_grade_delivery_uses_A3_config_not_hardcoded_recipients(self):
		policy = default_delivery_policy()
		policy['version'] = 'a3-delivery-test-2'
		policy['routes'][2]['recipients'] = ['group:watch', 'user:42']
		policy['routes'][2]['channels'] = ['SCREEN', 'PUSH']
		save_delivery_policy(policy, self.file)
		self.assertEqual(load_delivery_policy(self.file), policy)
		decision = evaluate_event(EVENT, policy=demo_policy())
		cmd = build_s3_alert_command(EVENT, decision, delivery_policy=policy)
		self.assertEqual(cmd['payload']['severity'], 'HIGH')
		self.assertEqual(cmd['payload']['recipients'], ['group:watch', 'user:42'])
		self.assertEqual(cmd['payload']['channels'], ['SCREEN', 'PUSH'])
		self.assertEqual(cmd['payload']['event_id'], EVENT['event_id'])
		self.assertEqual(cmd['schema'], 'ods.s3.alert.command')
		self.assertNotIn('grade', cmd['payload'])  # The S3 contract has severity, not A3 grade.

	def test_version_is_immutable_for_edits(self):
		p = default_delivery_policy()
		save_delivery_policy(p, self.file)
		p['routes'][0]['channels'] = ['SCREEN', 'PUSH']
		with self.assertRaises(ValueError):
			save_delivery_policy(p, self.file)
		p['version'] = 'a3-delivery-demo-2'
		save_delivery_policy(p, self.file)

	def test_invalid_recipients_channels_and_grades_rejected(self):
		cases = [
			(0, 'recipients', ['email:team']),
			(1, 'channels', ['EMAIL']),
			(1, 'recipients', []),
			(2, 'channels', ['SCREEN', 'SCREEN']),
			(0, 'grade', 2),
		]
		for index, name, value in cases:
			with self.subTest(name=name, value=value), self.assertRaises(ValueError):
				p = default_delivery_policy()
				p['routes'][index][name] = value
				validate_delivery_policy(p)

	def test_matrix_non_alert_must_not_be_sent(self):
		decision = evaluate_event({**EVENT, 'status': 'UNCERTAIN'}, policy=demo_policy())
		with self.assertRaises(ValueError):
			resolve_delivery(decision)
		with self.assertRaises(ValueError):
			build_s3_alert_command(EVENT, decision)


class S3PersistenceAndSimulationTests(unittest.TestCase):
	def setUp(self):
		self.tmp = tempfile.TemporaryDirectory()
		self.store = S3AlertStore(path=str(Path(self.tmp.name) / 's3.sqlite'))
		self.mock = MockS3Client(self.store)
		self.decision = evaluate_event(EVENT, policy=demo_policy())
		self.command = build_s3_alert_command(EVENT, self.decision, delivery_policy=default_delivery_policy())

	def tearDown(self):
		self.tmp.cleanup()

	def test_mock_command_reply_event_and_query_by_event(self):
		reply = self.mock.submit(self.command, self.decision.grade)
		self.assertEqual(reply['status'], 'OK')
		self.assertEqual(len(self.store.list_alerts(event_id=EVENT['event_id'])), 1)
		self.assertEqual(self.store.list_alerts(event_id=EVENT['event_id'])[0]['state'], 'CREATED')
		self.assertEqual(self.store.list_requests()[0]['status'], 'CONFIRMED')
		self.assertEqual(self.mock.submit(self.command, self.decision.grade)['status'], 'DUPLICATE')
		self.assertEqual(len(self.store.list_alerts()), 1)
		self.assertEqual(self.store.list_alerts(event_id=str(uuid4())), [])

	def test_mock_changes_state_via_s3_event_not_a3_action(self):
		alert_id = self.mock.submit(self.command, self.decision.grade)['alert_id']
		self.mock.update_mock_state(alert_id, 'ACKNOWLEDGED')
		self.assertEqual(self.store.list_alerts()[0]['state'], 'ACKNOWLEDGED')
		self.assertEqual(self.store.list_alerts()[0]['acknowledged_by'], 'user:mock-operator')
		self.mock.update_mock_state(alert_id, 'CLOSED')
		self.assertEqual(self.store.list_alerts()[0]['state'], 'CLOSED')
		with self.assertRaises(ValueError):
			self.mock.update_mock_state(alert_id, 'CREATED')

	def test_s3_rejected_and_unknown_are_different_states(self):
		self.store.record_attempt(self.command, self.decision.grade)
		self.assertEqual(self.store.record_reply(self.command['message_id'],
			reply={'status': 'ERROR', 'error': {'code': 'INVALID_CHANNEL', 'message': 'Unsupported'}}), 'REJECTED')
		self.assertEqual(self.store.list_requests()[0]['status'], 'REJECTED')
		self.assertIn('INVALID_CHANNEL', self.store.list_requests()[0]['error'])
		command2 = build_s3_alert_command({**EVENT, 'event_id': str(uuid4())}, self.decision)
		self.store.record_attempt(command2, self.decision.grade)
		self.assertEqual(self.store.record_reply(command2['message_id'], error=TimeoutError('not confirmed')), 'UNKNOWN')
		self.assertEqual(self.store.list_requests()[0]['status'], 'UNKNOWN')
		self.assertFalse(self.store.record_attempt(command2, self.decision.grade))

	def test_s3_received_state_ordering_and_validation(self):
		reply = self.mock.submit(self.command, self.decision.grade)
		old = self.store.list_alerts()[0]
		new_payload = {**old, 'state': 'ACKNOWLEDGED', 'acknowledged_by': 'user:maria',
			'acknowledged_at': '2026-10-17T08:00:00Z'}
		new_when = (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat()
		new_message = _mock_envelope(new_payload, new_when, sequence=9)
		self.assertTrue(self.store.record_event(new_message))
		stale = _mock_envelope({**old, 'state': 'CREATED'}, '2026-10-17T07:00:00Z')
		self.assertFalse(self.store.record_event(stale))
		self.assertEqual(self.store.list_alerts()[0]['state'], 'ACKNOWLEDGED')
		self.assertFalse(self.store.record_event(new_message))
		bad = dict(new_message)
		bad['producer'] = 'A3'
		with self.assertRaises(ValueError):
			self.store.record_event(bad)
		bad = dict(new_message)
		bad.pop('session_id')
		with self.assertRaises(ValueError):
			self.store.record_event(bad)

	def test_event_correlated_by_command_without_event_id(self):
		self.store.record_attempt(self.command, self.decision.grade)
		payload = {**self.command['payload'], 'alert_id': str(uuid4()),
			'state': 'CREATED', 'created_at': '2026-10-17T09:00:00Z'}
		payload.pop('event_id')
		self.store.record_event({**_mock_envelope(payload, '2026-10-17T09:00:00Z'),
			'correlation_id': self.command['message_id']})
		self.assertEqual(self.store.list_alerts(event_id=EVENT['event_id'])[0]['event_id'], EVENT['event_id'])
		self.assertEqual(self.store.list_requests()[0]['status'], 'CONFIRMED')

	def test_s3_query_reconciliation_does_not_replay_commands(self):
		reply = self.mock.submit(self.command, self.decision.grade)
		current = self.store.list_alerts()[0]
		self.store.ingest_query_reply({'status': 'OK', 'alerts': [{**current, 'state': 'CLOSED',
			'closed_at': '2026-10-17T10:00:00Z', 'closed_by': 'user:maria'}]})
		self.assertEqual(self.store.list_alerts()[0]['state'], 'CLOSED')
		self.assertEqual(len(self.store.list_requests()), 1)
		with self.assertRaises(RuntimeError):
			self.store.ingest_query_reply({'status': 'ERROR', 'error': {'code': 'INVALID_QUERY'}})

	def test_mock_all_three_events_only_once(self):
		for scenario in get_scenarios():
			if scenario['case'] != 'VIOLATION':
				continue
			event = scenario['envelope']['payload']
			decision = get_mock_decision(scenario)
			self.mock.submit(build_s3_alert_command(event, decision), decision.grade)
		self.assertEqual(len(self.store.list_alerts()), 3)
		self.assertEqual({a['severity'] for a in self.store.list_alerts()}, {'HIGH', 'MEDIUM'})


class S3ClientAndEngineTests(unittest.TestCase):
	def setUp(self):
		self.tmp = tempfile.TemporaryDirectory()
		self.path = str(Path(self.tmp.name) / 'events.sqlite')
		self.policy = patch('services.kafka_engine.load_policy', return_value=demo_policy())
		self.policy.start()

	def tearDown(self):
		self.policy.stop()
		self.tmp.cleanup()

	def test_build_query_uses_only_contract_filters(self):
		query = build_alerts_query(state='CLOSED', severity='HIGH', limit=500,
			from_at='2026-09-01T00:00:00Z', to_at='2026-10-01T00:00:00Z')
		self.assertEqual(query['schema'], ALERT_QUERY)
		self.assertEqual(query['message_type'], 'query')
		self.assertEqual(query['target'], 'S3')
		self.assertNotIn('event_id', query['payload'])
		with self.assertRaises(ValueError):
			build_alerts_query(limit=501)
		with self.assertRaises(ValueError):
			build_alerts_query(from_at='2026-10-01T00:00:00Z', to_at='2026-09-01T00:00:00Z')

	def test_query_calls_s3_and_records_authoritative_history(self):
		store = S3AlertStore(path=str(Path(self.tmp.name) / 's3-store.sqlite'))
		mock = MockS3Client(store)
		decision = evaluate_event(EVENT, policy=demo_policy())
		command = build_s3_alert_command(EVENT, decision)
		mock.submit(command, decision.grade)
		alert = store.list_alerts()[0]
		another = S3AlertStore(path=str(Path(self.tmp.name) / 'other.sqlite'))
		with patch('services.s3_alerts.request_reply', return_value={
			'status': 'OK', 'alerts': [alert], 'next_from_at': None}) as send:
			result = S3Client(brokers='fake:9092', store=another).history(severity='HIGH', limit=25)
		self.assertEqual(len(result['alerts']), 1)
		self.assertEqual(len(another.list_alerts()), 1)
		args, kwargs = send.call_args
		self.assertEqual(args[1], ALERT_QUERY)
		self.assertEqual(args[0]['payload'], {'limit': 25, 'severity': 'HIGH'})

	def test_engine_receives_s3_events_and_persists_under_event_id(self):
		engine = A3KafkaEngine(history_path=self.path, publish_alerts=True)
		decision = evaluate_event(EVENT, policy=demo_policy())
		command = build_s3_alert_command(EVENT, decision)
		alert_id = str(uuid4())
		with patch.object(engine, '_send_alert', return_value={'status': 'OK', 'alert_id': alert_id}):
			engine.process_message(s2_event())
		self.assertEqual(engine.s3_store.list_requests()[0]['status'], 'CONFIRMED')
		self.assertEqual(engine.history.get(EVENT['event_id'])['alerted_grade'], 3)
		payload = {**command['payload'], 'alert_id': alert_id, 'state': 'CREATED',
			'created_at': '2026-10-17T10:00:00Z'}
		self.assertTrue(engine.process_message(_mock_envelope(payload, '2026-10-17T10:00:00Z')))
		self.assertEqual(engine.s3_store.list_alerts(event_id=EVENT['event_id'])[0]['state'], 'CREATED')
		self.assertEqual(len(engine.snapshot_violations()), 1)

	def test_engine_rejected_reply_is_not_success_and_not_auto_retried(self):
		engine = A3KafkaEngine(history_path=self.path, publish_alerts=True)
		with patch.object(engine, '_send_alert', return_value={'status': 'ERROR',
			'error': {'code': 'INVALID_RECIPIENT', 'message': 'unknown'}}) as send:
			engine.process_message(s2_event())
			engine.process_message(s2_event())
			self.assertEqual(send.call_count, 1)
		self.assertEqual(engine.s3_store.list_requests()[0]['status'], 'REJECTED')
		self.assertEqual(engine.history.get(EVENT['event_id'])['alerted_grade'], 0)
		self.assertEqual(engine.history.get(EVENT['event_id'])['attempted_grade'], 3)

	def test_engine_timeout_is_unknown_and_not_auto_retried(self):
		engine = A3KafkaEngine(history_path=self.path, publish_alerts=True)
		with patch.object(engine, '_send_alert', side_effect=TimeoutError('broker response lost')) as send:
			engine.process_message(s2_event())
			engine.process_message(s2_event())
			self.assertEqual(send.call_count, 1)
		self.assertEqual(engine.s3_store.list_requests()[0]['status'], 'UNKNOWN')
		self.assertEqual(engine.history.get(EVENT['event_id'])['alerted_grade'], 0)


if __name__ == '__main__':
	unittest.main()


class S3DeepIntegrationTests(unittest.TestCase):
	def test_remote_history_by_event_paginates_all_alerts(self):
		with tempfile.TemporaryDirectory() as tmp:
			store = S3AlertStore(path=str(Path(tmp) / 'history.sqlite'))
			identifier = str(uuid4())
			other = str(uuid4())
			def make_alert(event_id):
				return {'alert_id': str(uuid4()), 'event_id': event_id,
					'title': 'Zone alert', 'message': 'Entered zone',
					'severity': 'HIGH', 'recipients': ['role:supervisor'],
					'channels': ['SCREEN'], 'state': 'CREATED',
					'created_at': '2026-10-17T10:00:00Z'}
			a = make_alert(identifier)
			b = make_alert(identifier)
			c = make_alert(other)
			first = {'status': 'OK', 'alerts': [a, c], 'next_from_at': '2026-10-17T10:00:01Z'}
			second = {'status': 'OK', 'alerts': [b], 'next_from_at': None}
			with patch('services.s3_alerts.request_reply', side_effect=[first, second]) as rpc:
				result = S3Client('fake:9092', store).history_by_event(identifier, limit=2)
			self.assertEqual(len(result['alerts']), 2)
			self.assertTrue(result['complete'])
			self.assertEqual(result['pages_checked'], 2)
			self.assertEqual(len(store.list_alerts()), 3)
			args, _ = rpc.call_args
			self.assertEqual(args[0]['payload']['from_at'], first['next_from_at'])
			self.assertNotIn('event_id', args[0]['payload'])

	def test_query_pagination_limit_is_disclosed(self):
		with tempfile.TemporaryDirectory() as tmp:
			store = S3AlertStore(path=str(Path(tmp) / 'history.sqlite'))
			with patch('services.s3_alerts.request_reply', return_value={
				'status': 'OK', 'alerts': [], 'next_from_at': '2026-10-17T09:00:00Z'}) as rpc:
				result = S3Client('fake:9092', store).history_by_event(str(uuid4()), limit=10, max_pages=2)
			self.assertFalse(result['complete'])
			self.assertEqual(result['pages_checked'], 2)
			self.assertEqual(result['alerts'], [])
			self.assertEqual(rpc.call_count, 2)

	def test_same_event_id_without_command_correlation_does_not_confirm_request(self):
		with tempfile.TemporaryDirectory() as tmp:
			store = S3AlertStore(path=str(Path(tmp) / 'history.sqlite'))
			decision = evaluate_event(EVENT, policy=demo_policy())
			command = build_s3_alert_command(EVENT, decision)
			store.record_attempt(command, decision.grade)
			payload = {**command['payload'], 'alert_id': str(uuid4()), 'state': 'CREATED',
				'created_at': '2026-10-17T10:00:00Z'}
			store.record_event(_mock_envelope(payload, '2026-10-17T10:00:00Z'))
			self.assertEqual(store.list_requests()[0]['status'], 'PENDING')
			store.record_event({**_mock_envelope(payload, '2026-10-17T10:00:01Z'),
				'correlation_id': command['message_id']})
			self.assertEqual(store.list_requests()[0]['status'], 'CONFIRMED')
			self.assertEqual(store.record_reply(command['message_id'], error=TimeoutError('late reply')),
				'CONFIRMED')

	def test_real_alert_command_rpc_correlates_own_reply(self):
		import sys
		import types
		from services.kafka_rpc import request_reply
		queue = []
		sent = []

		class Message:
			def __init__(self, content):
				self.content = content
			def error(self):
				return None
			def value(self):
				return json.dumps(self.content).encode('utf-8')

		class Consumer:
			def __init__(self, settings):
				self.settings = settings
			def subscribe(self, topics):
				self.topics = topics
			def assignment(self):
				return [1]
			def poll(self, seconds):
				return queue.pop(0) if queue else None
			def close(self):
				pass

		class Producer:
			def __init__(self, settings):
				self.settings = settings
			def produce(self, topic, body):
				command = json.loads(body)
				sent.append((topic, command))
				for producer, correlation, answer in [
					('S3', str(uuid4()), 'wrong correlation'),
					('S4', command['message_id'], 'wrong producer'),
					('S3', command['message_id'], 'accepted'),
				]:
					queue.append(Message({'schema': topic + '.reply', 'message_type': 'reply',
						'producer': producer, 'correlation_id': correlation,
						'payload': {'status': 'OK', 'alert_id': str(uuid4()), 'comment': answer}}))
			def flush(self, timeout):
				return 0
		cmd = build_s3_alert_command(EVENT, evaluate_event(EVENT, policy=demo_policy()))
		with patch.dict(sys.modules, {'confluent_kafka': types.SimpleNamespace(Consumer=Consumer, Producer=Producer)}):
			reply = request_reply(cmd, 'ods.s3.alert.command', brokers='fake:9092', timeout=1.0)
		self.assertEqual(reply['comment'], 'accepted')
		self.assertEqual(sent[0][0], 'ods.s3.alert.command')
		self.assertEqual(sent[0][1]['target'], 'S3')


class S3DeliveryFailureRegressionTests(unittest.TestCase):
	def test_invalid_delivery_configuration_does_not_mark_command_attempted(self):
		with tempfile.TemporaryDirectory() as tmp:
			path = str(Path(tmp) / 'events.sqlite')
			with patch('services.kafka_engine.load_policy', return_value=demo_policy()):
				engine = A3KafkaEngine(history_path=path, publish_alerts=True)
				with patch('services.kafka_engine.build_s3_alert_command', side_effect=ValueError('Invalid delivery policy')):
					engine.process_message(s2_event())
				self.assertEqual(engine.history.get(EVENT['event_id'])['attempted_grade'], 0)
				self.assertEqual(engine.s3_store.list_requests(), [])
				updated = {**EVENT, 'phase': 'UPDATED', 'duration_ms': EVENT['duration_ms'] + 500}
				with patch.object(engine, '_send_alert', return_value={'status': 'OK', 'alert_id': str(uuid4())}) as send:
					engine.process_message(s2_event(updated))
					self.assertEqual(send.call_count, 1)
				self.assertEqual(engine.history.get(EVENT['event_id'])['attempted_grade'], 3)
				self.assertEqual(engine.s3_store.list_requests()[0]['status'], 'CONFIRMED')
