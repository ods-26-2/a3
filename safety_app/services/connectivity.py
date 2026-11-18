"""Read-only A3 network diagnostics: TCP reachability and Kafka metadata.

Only called by the operator from the dedicated Streamlit diagnostics page.
No consumer starts, no messages are published and no topic is created.
"""
import socket
from dataclasses import dataclass

from services.s1_integration import kafka_settings_from_env


@dataclass(frozen=True)
class ConnectionResult:
	address: str
	tcp_ok: bool
	tcp_detail: str
	kafka_ok: bool
	kafka_detail: str


def check_tcp(host: str, port: int, timeout: float = 2.0):
	"""Test whether TCP accepts a connection (not whether Kafka works)."""
	if not host or not host.strip():
		raise ValueError('Host is required')
	if not 1 <= port <= 65535:
		raise ValueError('Port must be between 1 and 65535')
	if not 0 < timeout <= 15:
		raise ValueError('Timeout must be within (0, 15] seconds')
	try:
		with socket.create_connection((host.strip(), port), timeout=timeout):
			return True, 'Conexão TCP estabelecida.'
	except OSError as exc:
		return False, f'{type(exc).__name__}: {exc}'


def check_kafka_metadata(host: str, port: int, timeout: float = 3.0):
	"""Request Kafka metadata without subscribing, producing or mutating topics."""
	try:
		from confluent_kafka.admin import AdminClient
	except ImportError:
		return False, 'confluent-kafka não está instalado.'
	try:
		config = {**kafka_settings_from_env(), 'bootstrap.servers': f'{host.strip()}:{port}',
			'client.id': 'a3-connectivity-check', 'socket.timeout.ms': int(timeout * 1000),
			'api.version.request.timeout.ms': int(timeout * 1000)}
		admin = AdminClient(config)
		metadata = admin.list_topics(timeout=timeout)
		if metadata is None or not getattr(metadata, 'brokers', None):
			return False, 'Kafka não retornou metadados de brokers.'
		return True, f'Metadados Kafka recebidos ({len(metadata.brokers)} broker(s)).'
	except Exception as exc:
		return False, f'{type(exc).__name__}: {exc}'


def probe_broker(host: str, port: int, tcp_timeout: float = 2.0, kafka_timeout: float = 3.0):
	"""Check one endpoint without altering application state or Kafka data."""
	ok, detail = check_tcp(host, port, tcp_timeout)
	if not ok:
		return ConnectionResult(f'{host}:{port}', False, detail, False,
			'Metadados Kafka não consultados: acesso TCP indisponível.')
	kafka_ok, kafka_detail = check_kafka_metadata(host, port, kafka_timeout)
	return ConnectionResult(f'{host}:{port}', True, detail, kafka_ok, kafka_detail)
