"""Read-only public Kafka connectivity tests. All network operations are mocked."""
import unittest
from unittest.mock import patch

from services.connectivity import ConnectionResult, check_tcp, probe_broker
from services.kafka_config import PUBLIC_KAFKA_HOST, PUBLIC_KAFKA_PORTS


class PublicConnectivityTests(unittest.TestCase):
	def test_public_host_and_ports_are_explicit(self):
		self.assertEqual(PUBLIC_KAFKA_HOST, '200.17.66.56')
		self.assertEqual(PUBLIC_KAFKA_PORTS, (9092, 9093))

	def test_tcp_accepts_socket_connection(self):
		with patch('services.connectivity.socket.create_connection') as connect:
			connect.return_value.__enter__.return_value = object()
			self.assertEqual(check_tcp('200.17.66.56', 9092)[0], True)
			connect.assert_called_once_with(('200.17.66.56', 9092), timeout=2.0)

	def test_tcp_errors_are_reported_without_raising(self):
		with patch('services.connectivity.socket.create_connection', side_effect=TimeoutError('timeout')):
			status, message = check_tcp('200.17.66.56', 9093)
			self.assertFalse(status)
			self.assertIn('timeout', message)

	def test_probe_does_not_request_kafka_on_tcp_failure(self):
		with patch('services.connectivity.check_tcp', return_value=(False, 'closed')):
			with patch('services.connectivity.check_kafka_metadata') as kafka:
				result = probe_broker('200.17.66.56', 9092)
				self.assertFalse(result.kafka_ok)
				kafka.assert_not_called()

	def test_probe_reports_tcp_and_kafka_independently(self):
		with patch('services.connectivity.check_tcp', return_value=(True, 'connected')):
			with patch('services.connectivity.check_kafka_metadata', return_value=(False, 'not kafka')):
				result = probe_broker('200.17.66.56', 9093)
				self.assertTrue(result.tcp_ok)
				self.assertFalse(result.kafka_ok)
				self.assertEqual(result.address, '200.17.66.56:9093')

	def test_rejects_invalid_port(self):
		with self.assertRaises(ValueError):
			check_tcp('200.17.66.56', 65536)


if __name__ == '__main__':
	unittest.main()
