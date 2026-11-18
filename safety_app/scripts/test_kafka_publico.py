"""Manual read-only A3 TCP/Kafka connectivity test (no Streamlit required).

Run from safety_app:
    python scripts/test_kafka_publico.py
    python scripts/test_kafka_publico.py --host 200.17.66.56 --ports 9092,9093

Only performs TCP connections and read-only Kafka metadata queries.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.connectivity import probe_broker
from services.kafka_config import PUBLIC_KAFKA_HOST, PUBLIC_KAFKA_PORTS


def parse_ports(value: str) -> tuple[int, ...]:
	ports = tuple(int(part.strip()) for part in value.split(',') if part.strip())
	if not ports or any(not 1 <= port <= 65535 for port in ports):
		raise argparse.ArgumentTypeError('Enter one or more valid ports separated by commas.')
	return ports


def main() -> int:
	parser = argparse.ArgumentParser(description='A3 read-only TCP/Kafka metadata diagnostic')
	parser.add_argument('--host', default=PUBLIC_KAFKA_HOST, help='Broker IP or hostname')
	parser.add_argument('--ports', type=parse_ports,
		default=PUBLIC_KAFKA_PORTS, help='Comma-separated TCP ports (default: 9092,9093)')
	parser.add_argument('--timeout', type=float, default=3.0,
		help='Timeout in seconds for TCP and Kafka metadata (default: 3)')
	args = parser.parse_args()
	if not 0 < args.timeout <= 15:
		parser.error('--timeout must be greater than 0 and at most 15')
	if not args.host.strip():
		parser.error('--host cannot be empty')

	print('A3 Kafka connectivity diagnostic (read-only)')
	print('NOTE: this does not start S1/S2/S3/S4, subscribe, or produce messages.')
	results = []
	for port in args.ports:
		result = probe_broker(args.host.strip(), port,
			tcp_timeout=args.timeout, kafka_timeout=args.timeout)
		results.append(result)
		print(f'\nEndpoint: {result.address}')
		print(f'TCP: {"OK" if result.tcp_ok else "FAIL"} - {result.tcp_detail}')
		print(f'Kafka: {"OK" if result.kafka_ok else "NOT CONFIRMED"} - {result.kafka_detail}')

	if any(result.kafka_ok for result in results):
		print('\nResult: Kafka metadata confirmed for at least one endpoint.')
		return 0
	if any(result.tcp_ok for result in results):
		print('\nResult: TCP accessible, but Kafka metadata not confirmed.')
		return 1
	print('\nResult: neither port accepted TCP; Kafka metadata was not tested.')
	return 2


if __name__ == '__main__':
	raise SystemExit(main())
