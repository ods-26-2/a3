"""Public 6c RULES and ROUTING contracts; files and brokers are never modified."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.qualification_rules import build_s2_rules_command, validate_s2_rules, send_s2_rules
from services.routing_policy import validate_routing
from services.shared_artifacts import register_artifact, publish_registered

RULES = {'rules': [{'rule_id': 'a3-entry', 'event_type': 'ZONE_ENTER',
	'zone_id': 'a3-zone-01', 'class': 'person', 'min_confidence': 0.7,
	'uncertain_below': 0.85, 'min_duration_ms': 500, 'debounce_ms': 500}]}
ROUTING = {'routes': [{'event_type': 'ZONE_ENTER', 'min_status': 'CONFIRMED',
	'severity': 'HIGH', 'recipients': ['role:supervisor'], 'channels': ['SCREEN'],
	'title': 'Alert', 'message': 'Zone {zone_id}', 'capture': True}]}


class QualifyingRoutingContracts(unittest.TestCase):
	def test_s2_rules_command_separate_from_matrix(self):
		cmd = build_s2_rules_command(RULES['rules'], 'rules-a3-1')
		self.assertEqual(cmd['target'], 'S2')
		self.assertEqual(cmd['schema'], 'ods.s2.rules.command')
		self.assertEqual(cmd['payload']['artifact_version'], 'rules-a3-1')
		self.assertNotIn('severity', str(cmd))
		with patch('services.qualification_rules.request_reply', return_value={'status': 'OK'}) as mock:
			self.assertEqual(send_s2_rules(RULES['rules'], 'rules-a3-1', 'broker:9092')['reply']['status'], 'OK')
			self.assertTrue(mock.called)

	def test_s2_rules_validation_requires_catalog_and_threshold(self):
		with self.assertRaises(ValueError):
			validate_s2_rules({'rules': [{'rule_id': 'test', 'event_type': 'DWELL_EXCEEDED'}]})
		with self.assertRaises(ValueError):
			validate_s2_rules({'rules': [{'rule_id': 'test', 'event_type': 'UNKNOWN'}]})

	def test_routing_requirements(self):
		self.assertEqual(validate_routing(ROUTING), ROUTING)
		bad = json.loads(json.dumps(ROUTING))
		bad['routes'][0]['channels'] = ['MP4']
		with self.assertRaises(ValueError):
			validate_routing(bad)

	def test_immutable_register_and_publish_6c(self):
		with tempfile.TemporaryDirectory() as temp:
			one = register_artifact('RULES', RULES, temp)
			two = register_artifact('ROUTING', ROUTING, temp)
			self.assertEqual(one['version'], 'rules-a3-1')
			self.assertEqual(two['version'], 'routing-a3-1')
			self.assertNotEqual(one['message']['payload']['kind'], two['message']['payload']['kind'])
			self.assertIn('registry://', two['message']['payload']['uri'])
			with patch('services.shared_artifacts._send', return_value=True) as send:
				msg = publish_registered(temp, two['version'], 'broker:9092')
				self.assertEqual(msg['schema'], 'ods.artifact.published')
				self.assertEqual(send.call_count, 1)
			Path(two['path']).write_text('MODIFIED', encoding='utf-8')
			with self.assertRaises(ValueError):
				publish_registered(temp, two['version'], 'broker:9092')
