"""Kafka broker configuration for A3.

A3 defaults to the public gateway supplied for connectivity checks. No network
operation occurs when this module is imported; live consumption is opt-in.
"""
import os

PUBLIC_KAFKA_HOST = '200.17.66.56'
PUBLIC_KAFKA_PORTS = (9092, 9093)
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = f'{PUBLIC_KAFKA_HOST}:9092'
PRIVATE_KAFKA_BOOTSTRAP_SERVERS = '172.18.4.56:9092'


def get_kafka_bootstrap_servers():
	"""Return A3 broker address; an explicitly empty override disables it."""
	return os.getenv('A3_KAFKA_BOOTSTRAP_SERVERS', DEFAULT_KAFKA_BOOTSTRAP_SERVERS).strip()
