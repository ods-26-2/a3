"""Regressions for immutable commands, S2 lifecycle and demo/live source switches."""
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from services.kafka_engine import A3KafkaEngine
from services.qualification_rules import validate_s2_rules
from services.risk_policy import demo_policy
from services.shared_artifacts import load_registered_rules, register_artifact

RULE = {'rule_id': 'a3-entry', 'event_type': 'ZONE_ENTER',
	'zone_id': 'a3-zona-restrita-1', 'class': 'person',
	'min_confidence': 0.7, 'uncertain_below': 0.85}
EVENT = {'event_id': str(uuid4()), 'event_type': 'ZONE_ENTER',
	'rule_id': 'a3-entry', 'zone_id': 'a3-zona-restrita-1',
	'entity_id': 'entity-a', 'class': 'person', 'status': 'CONFIRMED',
	'phase': 'STARTED', 'confidence': 0.96,
	'started_at': '2026-10-16T00:05:00Z', 'duration_ms': 500,
	'details': {'movement': 'ENTER'}, 'source_ids': ['cam0']}


def envelope(payload):
	return {'schema': 'ods.s2.event', 'message_type': 'event',
		'producer': 'S2', 'source_id': 'cam0', 'payload': payload}


class RulesAndLifecycleRegressionTests(unittest.TestCase):
	def test_s2_requires_both_confidence_fields_and_consistent_bounds(self):
		for incomplete in ({}, {'min_confidence': 0.5}, {'uncertain_below': 0.8},
			{'min_confidence': 0.9, 'uncertain_below': 0.7}):
			with self.subTest(incomplete=incomplete), self.assertRaises(ValueError):
				validate_s2_rules({'rules': [{**RULE, **incomplete,
					**({k: None for k in RULE if k in ('min_confidence','uncertain_below') and k not in incomplete})}]})
		self.assertEqual(len(validate_s2_rules({'rules': [RULE]})['rules']), 1)

	def test_registered_rules_cannot_be_replaced_or_falsely_versioned(self):
		with tempfile.TemporaryDirectory() as path:
			registered = register_artifact('RULES', {'rules': [RULE]}, path)
			self.assertEqual(load_registered_rules(path, registered['version']), [RULE])
			with self.assertRaises(ValueError):
				load_registered_rules(path, 'rules-a3-2')
			Path(registered['path']).write_text('{"rules":[]}', encoding='utf-8')
			with self.assertRaises(ValueError):
				load_registered_rules(path, registered['version'])

	def test_ended_preserves_grade_but_disables_alert(self):
		with tempfile.TemporaryDirectory() as d:
			with patch('services.kafka_engine.load_policy', return_value=demo_policy()):
				engine = A3KafkaEngine(history_path=str(Path(d) / 'history.sqlite'))
				first = engine.process_message(envelope(EVENT))
				self.assertEqual(first.grade, 3)
				ended = engine.process_message(envelope({**EVENT, 'phase': 'ENDED'}))
				self.assertEqual(ended.grade, 3)
				self.assertEqual(ended.action, 'MONITOR')
				self.assertEqual(engine.history.get(EVENT['event_id'])['phase'], 'ENDED')
				self.assertEqual(engine.history.get(EVENT['event_id'])['decision']['action'], 'MONITOR')

	def test_uncertain_update_does_not_keep_previous_confirmed_action(self):
		with tempfile.TemporaryDirectory() as d:
			with patch('services.kafka_engine.load_policy', return_value=demo_policy()):
				engine = A3KafkaEngine(history_path=str(Path(d) / 'history.sqlite'))
				self.assertEqual(engine.process_message(envelope(EVENT)).grade, 3)
				updated = engine.process_message(envelope({**EVENT, 'phase': 'UPDATED',
					'status': 'UNCERTAIN', 'duration_ms': 1000}))
				self.assertEqual(updated.action, 'REVIEW')
				self.assertEqual(updated.status, 'UNCERTAIN')
				self.assertEqual(engine.history.get(EVENT['event_id'])['decision']['grade'], 0)

	def test_invalid_replay_status_is_not_silently_accepted(self):
		with tempfile.TemporaryDirectory() as d:
			with patch('services.kafka_engine.load_policy', return_value=demo_policy()):
				engine = A3KafkaEngine(history_path=str(Path(d) / 'history.sqlite'))
				engine.process_message(envelope(EVENT))
				self.assertIsNone(engine.process_message(envelope({**EVENT, 'status': 'IMPOSSIBLE'})))
				self.assertIsNone(engine.process_message(envelope({**EVENT, 'phase': 'INVALID'})))
				self.assertEqual(engine.history.get(EVENT['event_id'])['phase'], 'STARTED')


if __name__ == '__main__':
	unittest.main()
