"""A3 contextual matrix, S2 event lifecycle, S3 commands and durable history.

No real Kafka, camera or external component is needed for these tests.
"""
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

from services.decision_matrix import evaluate_event
from services.kafka_engine import A3KafkaEngine, build_s3_alert_command
from services.mock_validation import get_scenarios, get_mock_matrix_evaluation, get_mock_decision
from services.risk_policy import demo_policy, default_policy, match_rule, validate_policy


EVENT = {
	'event_id': 'b64ce94f-0941-4686-9f89-77687a40bf50',
	'event_type': 'ZONE_ENTER', 'rule_id': 's2-rule-entry',
	'entity_id': 'entity-01', 'zone_id': 'a3-zona-restrita-1',
	'class': 'person', 'status': 'CONFIRMED', 'phase': 'STARTED',
	'confidence': 0.90, 'duration_ms': 1000,
	'started_at': '2026-10-16T00:05:00Z', 'source_ids': ['cam0'],
	'details': {'transition': 'ENTER'},
}


def envelope(payload):
	return {'schema': 'ods.s2.event', 'message_type': 'event',
		'producer': 'S2', 'source_id': 'cam0', 'payload': payload}


class DecisionMatrixTests(unittest.TestCase):
	def test_three_violations_calculated_not_hardcoded(self):
		violations = [s for s in get_scenarios() if s['case'] == 'VIOLATION']
		self.assertEqual(len(violations), 3)
		for scenario in violations:
			decision = get_mock_decision(scenario)
			self.assertGreater(decision.grade, 0)
			self.assertIn(decision.severity, ('LOW', 'MEDIUM', 'HIGH'))
			self.assertTrue(decision.matched_rule.startswith('a3-'))
		df = get_mock_matrix_evaluation()
		self.assertEqual(len(df), 3)
		self.assertTrue(df['Grau calculado'].between(1, 3).all())
		self.assertTrue(df['Justificativas'].str.len().gt(15).all())

	def test_zone_class_movement_policy(self):
		policy = demo_policy()
		self.assertEqual(evaluate_event(EVENT, policy=policy).grade, 3)
		person = {**EVENT, 'zone_id': 'a3-zona-armazenamento'}
		self.assertEqual(evaluate_event(person, policy=policy).grade, 1)
		vehicle = {**person, 'class': 'vehicle'}
		decision = evaluate_event(vehicle, policy=policy)
		self.assertEqual(decision.grade, 2)
		self.assertEqual(decision.matched_rule, 'a3-attention-vehicle-entry')

	def test_recurrence_and_occupancy_escalation(self):
		payload = {**EVENT, 'event_type': 'OCCUPANCY_ABOVE',
			'zone_id': 'a3-zona-armazenamento',
			'details': {'count': 3, 'max_occupancy': 2}}
		self.assertEqual(evaluate_event(payload, policy=demo_policy()).grade, 2)
		self.assertEqual(evaluate_event(payload, policy=demo_policy(), recurrence_count=2).grade, 3)
		payload['details'] = {'count': 4, 'max_occupancy': 2}
		self.assertEqual(evaluate_event(payload, policy=demo_policy()).grade, 3)

	def test_uncertain_unconfigured_ended_and_min_confidence(self):
		policy = demo_policy()
		for diff in ({'status': 'UNCERTAIN'}, {'phase': 'ENDED'},
			{'zone_id': 'unknown-zone'}):
			out = evaluate_event({**EVENT, **diff}, policy=policy)
			self.assertEqual(out.grade, 0)
			self.assertNotEqual(out.action, 'ALERT')
		policy['min_decision_confidence'] = 0.95
		self.assertEqual(evaluate_event(EVENT, policy=policy).action, 'REVIEW')
		with self.assertRaises(ValueError):
			evaluate_event({**EVENT, 'confidence': 3}, policy=policy)

	def test_rule_validation_and_ambiguous_tie(self):
		policy = demo_policy()
		policy['rules'].append({**policy['rules'][0], 'rule_id': 'duplicate',
			'object_class': 'vehicle', 'movement': '*'})
		policy['rules'].append({**policy['rules'][0], 'rule_id': 'duplicate-2',
			'object_class': '*', 'movement': 'ENTER'})
		with self.assertRaises(ValueError):
			match_rule(policy, 'SAFE', 'ZONE_ENTER', 'vehicle', 'ENTER')
		self.assertEqual(validate_policy(default_policy())['version'], 'a3-risk-1')

	def test_s3_contract_uses_original_event_id_and_stable_command_id(self):
		decision = evaluate_event(EVENT, policy=demo_policy())
		a = build_s3_alert_command(EVENT, decision)
		b = build_s3_alert_command(EVENT, decision)
		UUID(a['message_id'])
		self.assertEqual(a['message_id'], b['message_id'])
		self.assertEqual(a['schema'], 'ods.s3.alert.command')
		self.assertEqual(a['target'], 'S3')
		self.assertEqual(a['payload']['event_id'], EVENT['event_id'])
		self.assertEqual(a['payload']['severity'], 'HIGH')


class EngineHistoryTests(unittest.TestCase):
	def setUp(self):
		self.temp = tempfile.TemporaryDirectory()
		self.path = str(Path(self.temp.name) / 'a3_events.sqlite3')
		self.policy_patch = patch('services.kafka_engine.load_policy', return_value=demo_policy())
		self.policy_patch.start()

	def tearDown(self):
		self.policy_patch.stop()
		self.temp.cleanup()

	def engine(self, alerts=False):
		return A3KafkaEngine(brokers='', publish_alerts=alerts, history_path=self.path)

	def test_durable_event_id_updates_end_and_replay(self):
		engine = self.engine()
		p = deepcopy(EVENT)
		self.assertEqual(engine.process_message(envelope(p)).grade, 3)
		self.assertEqual(len(engine.snapshot_violations()), 1)
		p['phase'] = 'UPDATED'
		p['duration_ms'] = 3000
		self.assertEqual(engine.process_message(envelope(p)).grade, 3)
		p['duration_ms'] = 2000
		engine.process_message(envelope(p))
		self.assertEqual(engine.history.get(EVENT['event_id'])['duration_ms'], 3000)
		p['phase'] = 'ENDED'
		engine.process_message(envelope(p))
		self.assertEqual(engine.history.get(EVENT['event_id'])['phase'], 'ENDED')
		self.assertEqual(len(self.engine().snapshot_violations()), 1)
		self.assertEqual(len(self.engine().snapshot_decisions()), 1)
		self.assertIsNone(engine.process_message({**envelope(EVENT), 'producer': 'S1'}))

	def test_recurrence_only_previous_distinct_events_within_window(self):
		engine = self.engine()
		engine.process_message(envelope(EVENT))
		p = {**EVENT, 'event_id': str(uuid4()),
			'started_at': '2026-10-16T00:15:00Z', 'zone_id': 'a3-zona-armazenamento'}
		decision = engine.process_message(envelope(p))
		self.assertEqual(decision.recurrence_count, 1)
		p2 = {**p, 'event_id': str(uuid4()), 'started_at': '2026-10-16T00:25:00Z'}
		self.assertEqual(engine.process_message(envelope(p2)).recurrence_count, 2)
		p3 = {**p2, 'event_id': str(uuid4()), 'started_at': '2026-10-18T00:25:00Z'}
		self.assertEqual(engine.process_message(envelope(p3)).recurrence_count, 0)

	def test_alerts_disabled_and_ack_gate(self):
		engine = self.engine(alerts=False)
		with patch.object(engine, '_send_alert') as send:
			engine.process_message(envelope(EVENT))
			send.assert_not_called()
		# Different event so it has not already been processed in the same database.
		p = {**EVENT, 'event_id': str(uuid4()), 'started_at': '2026-10-16T00:15:00Z'}
		engine = self.engine(alerts=True)
		with patch.object(engine, '_send_alert', return_value={'status': 'OK', 'alert_id': str(uuid4())}) as send:
			engine.process_message(envelope(p))
			engine.process_message(envelope(p))
			self.assertEqual(send.call_count, 1)
			self.assertEqual(engine.history.get(p['event_id'])['alerted_grade'], 3)

	def test_ambiguous_s3_reply_never_claims_ack_or_resends_automatically(self):
		engine = self.engine(alerts=True)
		with patch.object(engine, '_send_alert', side_effect=TimeoutError('S3 unavailable')) as send:
			engine.process_message(envelope(EVENT))
			engine.process_message(envelope(EVENT))
			self.assertEqual(send.call_count, 1)
			self.assertEqual(engine.history.get(EVENT['event_id'])['alerted_grade'], 0)
			self.assertEqual(engine.history.get(EVENT['event_id'])['attempted_grade'], 3)
			self.assertIn('unconfirmed', engine.last_error)

	def test_entity_metadata_not_events(self):
		engine = self.engine()
		engine.process_message({'schema': 'ods.s2.entity', 'message_type': 'event',
			'producer': 'S2', 'payload': {'entities': [{'entity_id': 'e-1',
			'track_refs': [{'track_id': 't-2'}], 'class': 'vehicle'}]}})
		self.assertEqual(engine.entity_tracks('e-1'), ['t-2'])
		self.assertTrue(engine.snapshot_violations().empty)
		with self.assertRaises(RuntimeError):
			engine.start()


if __name__ == '__main__':
	unittest.main()
