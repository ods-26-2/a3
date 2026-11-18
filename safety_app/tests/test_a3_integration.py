"""A3-only tests: no Kafka network; no modifications to S1 or B6."""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.zones import confirm_zone_config, load_zones, to_s1_pixels, validate_zones
from services.s1_integration import build_artifact, register_artifact, send_announcement, publish_artifact


TRIANGLE = [{
	'zone_id': 'zone_1700000000_0', 'name': 'Restricted Area 1', 'type': 'DANGER',
	'points': [{'x': 0.1, 'y': 0.1}, {'x': 0.6, 'y': 0.1}, {'x': 0.2, 'y': 0.8}],
}]


class A3IntegrationTest(unittest.TestCase):
	def setUp(self):
		self.directory = tempfile.TemporaryDirectory()
		self.addCleanup(self.directory.cleanup)
		self.store = str(Path(self.directory.name) / 'private_a3' / 'zones_b6.json')
		self.env = patch.dict(os.environ, {'A3_ZONES_PATH': self.store})
		self.env.start()
		self.addCleanup(self.env.stop)

	def test_stable_ids_and_conversion(self):
		self.assertEqual(confirm_zone_config(TRIANGLE), 1)
		first = load_zones()[0]
		self.assertEqual(first['zone_id'], 'a3-restricted-area-1')
		new_export = [{**TRIANGLE[0], 'zone_id': 'zone_999999999_0'}]
		confirm_zone_config(new_export)
		self.assertEqual(load_zones()[0]['zone_id'], first['zone_id'])
		artifact = build_artifact('cam0', 1000, 800, enter_ms=100, exit_ms=200, classes=['person'], max_occupancy=2)
		self.assertEqual(artifact['coordinates'], 'PIXELS')
		self.assertEqual(artifact['source_ids'], ['cam0'])
		self.assertEqual(artifact['zones'][0]['polygon'][0], [100, 80])
		self.assertEqual(artifact['zones'][0]['classes'], ['person'])
		self.assertEqual(artifact['zones'][0]['max_occupancy'], 2)
		self.assertNotIn('type', artifact['zones'][0])

	def test_invalid_polygons_rejected(self):
		bad = [{**TRIANGLE[0], 'points': [{'x': 0, 'y': 0}, {'x': 1, 'y': 1}, {'x': 0, 'y': 1}, {'x': 1, 'y': 0}]}]
		with self.assertRaises(ValueError):
			validate_zones(bad)
		bad[0]['points'][0]['x'] = -0.1
		with self.assertRaises(ValueError):
			validate_zones(bad)

	def test_registration_and_announcement(self):
		confirm_zone_config(TRIANGLE)
		registry = Path(self.directory.name) / 'registry'
		artifact = build_artifact('cam0', 1920, 1080)
		result = register_artifact(artifact, registry)
		self.assertEqual(result['version'], 'zones-a3-1')
		self.assertFalse(result['announced'])
		message = result['message']
		self.assertEqual(message['schema'], 'ods.artifact.published')
		self.assertEqual(message['producer'], 'A3')
		self.assertEqual(message['payload']['uri'], 'registry://zones-a3-1.json')
		self.assertTrue(message['published_at'].endswith('Z'))
		self.assertNotIn('occurred_at', message)
		self.assertEqual(json.loads((registry / 'zones-a3-1.json').read_text()), artifact)
		self.assertEqual(register_artifact(artifact, registry)['version'], 'zones-a3-2')
		self.assertEqual(json.loads((registry / 'zones-a3-1.json').read_text()), artifact)

	def test_real_kafka_call_is_opt_in_and_mockable(self):
		confirm_zone_config(TRIANGLE)
		registry = Path(self.directory.name) / 'registry'
		artifact = build_artifact('cam0', 100, 100)
		with patch('services.s1_integration._send', return_value=True) as fake:
			result = publish_artifact(artifact, registry, bootstrap_servers=None)
			self.assertFalse(result['announced'])
			fake.assert_not_called()
			msg = send_announcement(registry, result['version'], 'local-broker:9092')
			self.assertNotEqual(msg['message_id'], result['message']['message_id'])
			self.assertEqual(msg['payload']['hash_sha256'], result['message']['payload']['hash_sha256'])
			fake.assert_called_once()
		(registry / 'zones-a3-1.json').write_text('{}')
		with patch('services.s1_integration._send') as fake:
			with self.assertRaises(ValueError):
				send_announcement(registry, result['version'], 'local-broker:9092')
			fake.assert_not_called()


if __name__ == '__main__':
	unittest.main()
