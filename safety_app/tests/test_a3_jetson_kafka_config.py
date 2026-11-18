"""A3-only Jetson Kafka configuration tests. All transports mocked; no network."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.clips import query_s4
from services.kafka_config import DEFAULT_KAFKA_BOOTSTRAP_SERVERS, get_kafka_bootstrap_servers
from services.kafka_engine import A3KafkaEngine


class JetsonKafkaConfigurationTests(unittest.TestCase):
	def test_default_broker_is_jetson(self):
		self.assertEqual(DEFAULT_KAFKA_BOOTSTRAP_SERVERS, '200.17.66.56:9092')
		with patch.dict('os.environ', {}, clear=True):
			self.assertEqual(get_kafka_bootstrap_servers(), '200.17.66.56:9092')

	def test_env_override(self):
		with patch.dict('os.environ', {'A3_KAFKA_BOOTSTRAP_SERVERS': ' custom.kafka:29092 '}):
			self.assertEqual(get_kafka_bootstrap_servers(), 'custom.kafka:29092')

	def test_empty_override_disables_broker(self):
		with patch.dict('os.environ', {'A3_KAFKA_BOOTSTRAP_SERVERS': ''}):
			self.assertEqual(get_kafka_bootstrap_servers(), '')

	def test_engine_uses_default_but_never_starts_without_explicit_start(self):
		with tempfile.TemporaryDirectory() as d:
			with patch.dict('os.environ', {}, clear=True):
				engine = A3KafkaEngine(history_path=str(Path(d) / 'events.sqlite'),
					s3_store_path=str(Path(d) / 's3.sqlite'))
				self.assertEqual(engine.brokers, DEFAULT_KAFKA_BOOTSTRAP_SERVERS)
				self.assertIsNone(engine._thread)

	def test_engine_explicit_empty_string_preserves_offline_simulation(self):
		with tempfile.TemporaryDirectory() as d:
				engine = A3KafkaEngine(brokers='', history_path=str(Path(d) / 'events.sqlite'),
						s3_store_path=str(Path(d) / 's3.sqlite'))
				self.assertEqual(engine.brokers, '')
				self.assertIsNone(engine._thread)

	def test_s4_real_query_uses_default_broker_without_connecting_in_test(self):
		with patch.dict('os.environ', {}, clear=True):
			with patch('services.kafka_rpc.request_reply', return_value={'status': 'OK', 'clips': []}) as rpc:
				self.assertEqual(query_s4(event_id='test-event'), [])
				self.assertEqual(rpc.call_args.kwargs['brokers'], DEFAULT_KAFKA_BOOTSTRAP_SERVERS)

	def test_s4_query_respects_environment_override(self):
		with patch.dict('os.environ', {'A3_KAFKA_BOOTSTRAP_SERVERS': 'test:9093'}):
			with patch('services.kafka_rpc.request_reply', return_value={'status': 'OK', 'clips': []}) as rpc:
				self.assertEqual(query_s4(event_id='test-event'), [])
				self.assertEqual(rpc.call_args.kwargs['brokers'], 'test:9093')


if __name__ == '__main__':
	unittest.main()
