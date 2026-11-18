"""Offline end-to-end acceptance tests for the A3 contract simulator."""
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

from services.integrated_simulator import (
	IntegratedEventSimulator, MockS4Boundary, build_scenario, scenario_options,
)
from services.clips import build_clip_query
from services.risk_policy import demo_policy
from services.mock_validation import get_scenarios


class IntegratedSimulatorTests(unittest.TestCase):
	def setUp(self):
		self.simulators = []

	def tearDown(self):
		for sim in self.simulators:
			sim.close()

	def create(self, key):
		sim = IntegratedEventSimulator(key)
		self.simulators.append(sim)
		return sim

	def test_all_seven_scenarios_finish_offline(self):
		for name in scenario_options():
			with self.subTest(scenario=name):
				sim = self.create(name)
				self.assertFalse(sim.complete)
				run = sim.run_all()
				self.assertTrue(sim.complete)
				self.assertEqual(len(run), len(sim.steps))
				self.assertIsNone(sim.advance())

	def test_entry_tracks_same_s2_event_through_started_updated_ended(self):
		sim = self.create('zone_entry')
		sim.run_all()
		rows = sim.engine.snapshot_all_events()
		self.assertEqual(len(rows), 1)
		self.assertEqual(rows[0]['phase'], 'ENDED')
		self.assertEqual(rows[0]['event_id'], get_scenarios()[0]['envelope']['payload']['event_id'])
		self.assertEqual(rows[0]['decision']['grade'], 3)
		self.assertEqual(rows[0]['decision']['policy_version'], demo_policy()['version'])
		self.assertEqual(rows[0]['decision']['action'], 'MONITOR')
		self.assertEqual(sim.trace[-1]['outcome'], 'Mensagem ignorada (repetida ou posterior ao encerramento).')

	def test_matrix_makes_decisions_instead_of_fixture_grades(self):
		for name, grades in [('zone_entry', [3]), ('occupancy', [2, 3]), ('dwell', [2, 3])]:
			with self.subTest(name=name):
				sim = self.create(name)
				sim.run_all()
				observed = [r['grade'] for r in sim.trace
					if r['kind'] == 'S2_EVENT' and r['action'] == 'ALERT']
				self.assertEqual(list(dict.fromkeys(observed)), grades)

	def test_s3_command_and_s3_events_are_correlated(self):
		sim = self.create('zone_entry')
		sim.run_all()
		requests = sim.engine.s3_store.list_requests()
		alerts = sim.engine.s3_store.list_alerts()
		self.assertEqual(len(requests), 1)
		self.assertEqual(len(alerts), 1)
		self.assertEqual(requests[0]['alert_id'], alerts[0]['alert_id'])
		self.assertEqual(requests[0]['event_id'], alerts[0]['event_id'])
		self.assertEqual(requests[0]['status'], 'CONFIRMED')
		self.assertEqual(alerts[0]['state'], 'ACKNOWLEDGED')
		self.assertEqual(alerts[0]['severity'], 'HIGH')
		self.assertEqual(alerts[0]['channels'], ['SCREEN'])
		self.assertEqual(alerts[0]['recipients'], ['role:supervisor'])
		commands = list(sim.sent_commands.values())
		self.assertEqual(len(commands), 1)
		self.assertEqual(commands[0]['schema'], 'ods.s3.alert.command')
		self.assertEqual(commands[0]['payload']['event_id'], alerts[0]['event_id'])
		self.assertNotIn('grade', commands[0]['payload'])

	def test_s3_alert_event_is_not_visible_before_created_stage(self):
		sim = self.create('zone_entry')
		sim.advance()  # entity
		sim.advance()  # STARTED: command/reply
		self.assertEqual(len(sim.engine.s3_store.list_requests()), 1)
		self.assertEqual(sim.engine.s3_store.list_alerts(), [])
		sim.advance()  # S3 publishes CREATED event
		self.assertEqual(len(sim.engine.s3_store.list_alerts()), 1)

	def test_occupancy_escalation_sends_two_distinct_grades_not_duplicates(self):
		sim = self.create('occupancy')
		sim.run_all()
		grades = {r['grade'] for r in sim.engine.s3_store.list_requests()}
		self.assertEqual(grades, {2, 3})
		self.assertEqual(len(sim.engine.s3_store.list_alerts()), 2)
		self.assertEqual(len({r['message_id'] for r in sim.engine.s3_store.list_requests()}), 2)

	def test_recurrence_escalates_only_after_prior_distinct_events(self):
		sim = self.create('recurrence')
		sim.run_all()
		events = sorted(sim.engine.snapshot_all_events(), key=lambda r: r['started_at'])
		self.assertEqual([e['decision']['recurrence_count'] for e in events], [0, 1, 2])
		self.assertEqual([e['decision']['grade'] for e in events], [1, 1, 2])
		self.assertEqual(len(sim.engine.s3_store.list_requests()), 1)
		self.assertEqual(sim.engine.s3_store.list_requests()[0]['event_id'], events[2]['event_id'])

	def test_s4_clip_query_is_empty_before_evidence_is_ready(self):
		sim = self.create('zone_entry')
		uid = next(iter(sim.event_ids))
		self.assertEqual(sim.query_clips(uid), [])
		for _ in range(4):
			sim.advance()
		self.assertEqual(len(sim.query_clips(uid)), 1)
		self.assertTrue(sim.query_clips(uid)[0]['media_uri'].startswith('s4://'))

	def test_s4_returns_two_descriptors_or_empty_without_media_transfer(self):
		for name, number in [('occupancy', 2), ('dwell', 0)]:
			with self.subTest(name=name):
				sim = self.create(name)
				sim.run_all()
				uid = next(iter(sim.event_ids))
				self.assertEqual(len(sim.query_clips(uid)), number)
				self.assertNotIn('video', str(sim.snapshot()).lower())

	def test_s4_rejects_invalid_query_contract(self):
		mock = MockS4Boundary()
		with self.assertRaises(ValueError):
			mock.query({'schema': 'fake', 'target': 'S4', 'payload': {'event_id': 'x'}})
		with self.assertRaises(ValueError):
			mock.query({'schema': 'ods.s4.clip.query', 'target': 'S4', 'payload': {'event_id': 'invalid'}})
		cmd = build_clip_query(event_id=get_scenarios()[0]['envelope']['payload']['event_id'])
		self.assertEqual(mock.query(cmd), {'status': 'OK', 'clips': []})

	def test_normal_s1_observations_do_not_generate_qualified_s2_events(self):
		for name in ('normal_occupied', 'normal_empty'):
			with self.subTest(name=name):
				sim = self.create(name)
				sim.run_all()
				self.assertEqual(sim.engine.snapshot_all_events(), [])
				self.assertEqual(sim.engine.s3_store.list_requests(), [])
				self.assertEqual(sim.engine.s3_store.list_alerts(), [])
				self.assertEqual(sim.event_ids, set())
				self.assertEqual(sim.trace[0]['message']['producer'], 'S1')

	def test_simulator_keeps_both_sqlite_stores_separate(self):
		sim1 = self.create('zone_entry')
		sim2 = self.create('zone_entry')
		self.assertNotEqual(sim1.engine.history.path, sim2.engine.history.path)
		self.assertNotEqual(sim1.engine.s3_store.path, sim2.engine.s3_store.path)
		sim1.run_all()
		self.assertEqual(sim2.engine.snapshot_all_events(), [])
		self.assertEqual(sim2.engine.s3_store.list_alerts(), [])
		self.assertTrue(Path(sim1.engine.history.path).is_file())

	def test_s2_ids_and_envelopes_have_correct_uuid_and_schema(self):
		for name in ('zone_entry', 'occupancy', 'dwell', 'recurrence', 'uncertain'):
			with self.subTest(name=name):
				steps = build_scenario(name)
				for step in steps:
					if step['kind'] != 'S2_EVENT':
						continue
					e = step['envelope']
					self.assertEqual(e['schema'], 'ods.s2.event')
					self.assertEqual(e['schema_version'], '1.0')
					self.assertEqual(e['producer'], 'S2')
					UUID(e['message_id'])
					UUID(e['payload']['event_id'])
					self.assertIn(e['payload']['phase'], ('STARTED', 'UPDATED', 'ENDED'))


	def test_uncertain_s2_event_requires_review_without_s3_command(self):
		sim = self.create('uncertain')
		sim.run_all()
		event = sim.engine.snapshot_all_events()[0]
		self.assertEqual(event['decision']['grade'], 0)
		self.assertEqual(sim.engine.s3_store.list_requests(), [])
		self.assertEqual(sim.engine.s3_store.list_alerts(), [])
		self.assertEqual(len(sim.query_clips(event['event_id'])), 1)
		self.assertEqual(sim.trace[1]['action'], 'REVIEW')

	def test_unknown_scenario_is_rejected(self):
		with self.assertRaises(ValueError):
			build_scenario('fake-scenario')

	def test_simulator_has_no_kafka_sender_or_background_consumer(self):
		sim = self.create('zone_entry')
		self.assertEqual(sim.engine.brokers, '')
		self.assertIsNone(sim.engine._thread)
		with patch.object(sim.engine, '_consume_loop', side_effect=AssertionError('Kafka must not run')):
			sim.run_all()
		self.assertIsNone(sim.engine._thread)


if __name__ == '__main__':
	unittest.main()
