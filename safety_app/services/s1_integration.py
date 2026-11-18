"""A3-only publisher for immutable ZONES descriptors consumed by S1.

Implements the architecture 6c `ods.artifact.published` message over the
Kafka service specified by TRA-1. Never imports, edits, or starts S1.
"""
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from services.zones import load_zones, to_s1_pixels


SCHEMA = 'ods.artifact.published'
DEFAULT_ARTIFACT_ID = 'zones-a3'


def utc_now():
	return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def build_artifact(source_id, image_width, image_height, enter_ms=500, exit_ms=1500, classes=None, max_occupancy=None):
	return to_s1_pixels(load_zones(), source_id, image_width, image_height,
		enter_hysteresis_ms=enter_ms, exit_hysteresis_ms=exit_ms,
		classes=classes, max_occupancy=max_occupancy)


def build_announcement(artifact_id, version, filename, sha256, published_at=None):
	if not isinstance(artifact_id, str) or not re.fullmatch(r'[a-z0-9_-]+', artifact_id):
		raise ValueError('Invalid artifact_id')
	if not isinstance(version, str) or not re.fullmatch(re.escape(artifact_id) + r'-[1-9][0-9]*', version):
		raise ValueError('Invalid artifact_version')
	if filename != f'{version}.json':
		raise ValueError('Invalid artifact filename')
	if not re.fullmatch(r'[0-9a-f]{64}', sha256):
		raise ValueError('Invalid SHA-256 digest')
	at = published_at or utc_now()
	return {
		'message_type': 'event',
		'message_id': str(uuid4()),
		'schema': SCHEMA,
		'schema_version': '1.0',
		'producer': 'A3',
		'published_at': at,
		'payload': {
			'artifact_id': artifact_id,
			'artifact_version': version,
			'kind': 'ZONES',
			'owner': 'A3',
			'uri': f'registry://{filename}',
			'hash_sha256': sha256,
			'valid_from': at,
			'schema_version': '1.0',
		}
	}


def _registry_dir(registry_dir):
	if not registry_dir or not str(registry_dir).strip():
		raise ValueError('Shared registry directory is required (S1_REGISTRY_DIR)')
	return Path(registry_dir).expanduser().resolve()


def _check_artifact(artifact):
	if not isinstance(artifact, dict) or artifact.get('coordinates') != 'PIXELS':
		raise ValueError('Expected ZONES with PIXELS coordinates')
	ids = artifact.get('source_ids')
	if not isinstance(ids, list) or len(ids) != 1 or not ids[0]:
		raise ValueError('PIXELS artifact requires exactly one source_id')
	zones = artifact.get('zones')
	if not isinstance(zones, list) or not zones:
		raise ValueError('ZONES requires at least one zone')
	for zone in zones:
		if not isinstance(zone, dict) or not all(k in zone for k in ('zone_id', 'name', 'polygon', 'enter_hysteresis_ms', 'exit_hysteresis_ms')):
			raise ValueError('Invalid ZONES zone')
		if len(zone['polygon']) < 3:
			raise ValueError('ZONES polygon requires three vertices')


def register_artifact(artifact, registry_dir, artifact_id=DEFAULT_ARTIFACT_ID):
	"""Persist an immutable ZONES version and descriptor; do NOT contact Kafka."""
	_check_artifact(artifact)
	if not isinstance(artifact_id, str) or not re.fullmatch(r'[a-z0-9_-]+', artifact_id):
		raise ValueError('Invalid artifact_id')
	root = _registry_dir(registry_dir)
	root.mkdir(parents=True, exist_ok=True)
	data = (json.dumps(artifact, indent=2, ensure_ascii=False, allow_nan=False) + '\n').encode('utf-8')
	sha256 = hashlib.sha256(data).hexdigest()
	for version_number in range(1, 1000000):
		version = f'{artifact_id}-{version_number}'
		path = root / f'{version}.json'
		try:
			with path.open('xb') as handle:
				handle.write(data)
			break
		except FileExistsError:
			continue
	else:
		raise RuntimeError('Artifact version limit exceeded')
	message = build_announcement(artifact_id, version, path.name, sha256)
	# This sidecar is owned by A3. S1 reads only the immutable JSON via registry://.
	announcement_file = root / f'{version}.announcement.json'
	with announcement_file.open('x', encoding='utf-8') as handle:
		json.dump(message, handle, indent=2, ensure_ascii=False)
	return {'version': version, 'path': str(path), 'message': message, 'announced': False}


def _send(message, bootstrap_servers, *, topic=SCHEMA, kafka_options=None, timeout=15):
	"""Confirm delivery to broker, NOT processing by S1."""
	if not isinstance(bootstrap_servers, str) or not bootstrap_servers.strip():
		raise ValueError('Kafka bootstrap servers must be configured explicitly')
	if topic != SCHEMA and not re.fullmatch(r'ods\.[a-z0-9_]+\.[a-z0-9_]+', topic):
		raise ValueError('Invalid Kafka topic')
	try:
		from confluent_kafka import Producer
	except ImportError as error:
		raise RuntimeError('Install confluent-kafka in the A3 environment') from error
	options = {'bootstrap.servers': bootstrap_servers.strip(), 'acks': 'all', 'enable.idempotence': True,
		'delivery.timeout.ms': int(timeout * 1000)}
	if kafka_options:
		options.update(kafka_options)
	producer = Producer(options)
	deliveries = []
	def ack(error, record):
		deliveries.append(error)
	producer.produce(topic=topic, key=message['payload']['artifact_id'].encode('utf-8'),
		value=json.dumps(message, ensure_ascii=False, allow_nan=False).encode('utf-8'), callback=ack)
	remaining = producer.flush(timeout)
	if remaining or len(deliveries) != 1 or deliveries[0] is not None:
		reason = deliveries[0] if deliveries else 'delivery timeout'
		raise RuntimeError(f'Kafka did not confirm delivery: {reason}')
	return True


def send_announcement(registry_dir, version, bootstrap_servers, *, topic=SCHEMA, kafka_options=None, timeout=15):
	"""Republish registered content using a NEW message_id (idempotent artifact version)."""
	if not isinstance(version, str) or not re.fullmatch(r'[a-z0-9_-]+-[1-9][0-9]*', version):
		raise ValueError('Invalid version')
	root = _registry_dir(registry_dir)
	path = root / f'{version}.json'
	sidecar = root / f'{version}.announcement.json'
	original = json.loads(sidecar.read_text(encoding='utf-8'))
	payload = original.get('payload')
	if not isinstance(payload, dict) or payload.get('artifact_version') != version or payload.get('uri') != f'registry://{path.name}':
		raise ValueError('Invalid saved descriptor')
	data = path.read_bytes()
	if hashlib.sha256(data).hexdigest() != payload.get('hash_sha256'):
		raise ValueError('Immutable artifact SHA-256 mismatch')
	_check_artifact(json.loads(data))
	# Keep original descriptor values and valid_from; only reissued transport
	# message identity and publishing instant change, as required by B1 idempotency.
	message = {**original, 'message_id': str(uuid4()), 'published_at': utc_now()}
	_send(message, bootstrap_servers, topic=topic, kafka_options=kafka_options, timeout=timeout)
	return message


def publish_artifact(artifact, registry_dir, artifact_id=DEFAULT_ARTIFACT_ID,
		bootstrap_servers=None, *, topic=SCHEMA, kafka_options=None, timeout=15):
	result = register_artifact(artifact, registry_dir, artifact_id)
	if bootstrap_servers:
		message = send_announcement(registry_dir, result['version'], bootstrap_servers,
			topic=topic, kafka_options=kafka_options, timeout=timeout)
		result.update(message=message, announced=True)
	return result


def kafka_settings_from_env():
	"""Pass non-secret broker security configuration via environment variables."""
	allowed = {
		'A3_KAFKA_SECURITY_PROTOCOL': 'security.protocol',
		'A3_KAFKA_SASL_MECHANISM': 'sasl.mechanism',
		'A3_KAFKA_SASL_USERNAME': 'sasl.username',
		'A3_KAFKA_SASL_PASSWORD': 'sasl.password',
		'A3_KAFKA_SSL_CA_LOCATION': 'ssl.ca.location',
	}
	return {target: os.environ[source] for source, target in allowed.items() if os.environ.get(source)}
