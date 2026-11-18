"""Immutable A3 RULES/ROUTING artifacts, with 6c publication descriptors.

Publication acknowledges broker delivery, NOT adoption by S2/S3. Consumers
must see the same shared registry and recognize the pinned artifact_id.
"""
import hashlib
import json
import re
from pathlib import Path
from uuid import uuid4

from services.s1_integration import utc_now, _send, kafka_settings_from_env
from services.qualification_rules import validate_s2_rules
from services.routing_policy import validate_routing

VALIDATORS = {'RULES': validate_s2_rules, 'ROUTING': validate_routing}
IDS = {'RULES': 'rules-a3', 'ROUTING': 'routing-a3'}


def register_artifact(kind, content, registry_dir):
	if kind not in VALIDATORS:
		raise ValueError('Unknown A3 artifact kind')
	content = VALIDATORS[kind](content)
	root = Path(registry_dir).expanduser().resolve()
	if not registry_dir or not str(registry_dir).strip():
		raise ValueError('Shared registry path is required')
	root.mkdir(parents=True, exist_ok=True)
	artifact_id = IDS[kind]
	data = (json.dumps(content, ensure_ascii=False, indent=2, allow_nan=False) + '\n').encode('utf-8')
	digest = hashlib.sha256(data).hexdigest()
	for number in range(1, 100000):
		version = f'{artifact_id}-{number}'
		path = root / (version + '.json')
		try:
			with path.open('xb') as fh:
				fh.write(data)
			break
		except FileExistsError:
			continue
	else:
		raise RuntimeError('A3 artifact version limit reached')
	at = utc_now()
	message = {'message_type': 'event', 'message_id': str(uuid4()),
		'schema': 'ods.artifact.published', 'schema_version': '1.0',
		'producer': 'A3', 'published_at': at,
		'payload': {'artifact_id': artifact_id, 'artifact_version': version,
			'kind': kind, 'owner': 'A3', 'uri': f'registry://{path.name}',
			'hash_sha256': digest, 'valid_from': at, 'schema_version': '1.0'}}
	with (root / (version + '.announcement.json')).open('x', encoding='utf-8') as fh:
		json.dump(message, fh, ensure_ascii=False, indent=2)
	return {'version': version, 'path': str(path), 'message': message}


def publish_registered(registry_dir, version, brokers):
	if not isinstance(version, str) or not re.fullmatch(r'(?:rules|routing)-a3-[1-9][0-9]*', version):
		raise ValueError('Unexpected A3 artifact version')
	root = Path(registry_dir).expanduser().resolve()
	message = json.loads((root / (version + '.announcement.json')).read_text(encoding='utf-8'))
	payload = message['payload']
	if payload.get('owner') != 'A3' or payload.get('artifact_version') != version:
		raise ValueError('Artifact descriptor mismatch')
	content = (root / (version + '.json')).read_bytes()
	if hashlib.sha256(content).hexdigest() != payload['hash_sha256']:
		raise ValueError('Artifact content modified after publication')
	VALIDATORS[payload['kind']](json.loads(content))
	message['message_id'] = str(uuid4())
	message['published_at'] = utc_now()
	_send(message, brokers, kafka_options=kafka_settings_from_env())
	return message


def load_registered_rules(registry_dir, version):
	"""Load only a registered RULES version after verifying its SHA-256."""
	if not isinstance(registry_dir, str) or not registry_dir.strip():
		raise ValueError('Shared registry path is required')
	if not isinstance(version, str) or not re.fullmatch(r'rules-a3-[1-9][0-9]*', version):
		raise ValueError('Invalid registered RULES version')
	root = Path(registry_dir).expanduser().resolve()
	try:
		desc = json.loads((root / (version + '.announcement.json')).read_text(encoding='utf-8'))
	except FileNotFoundError as exc:
		raise ValueError(f'RULES version {version} is not registered') from exc
	payload = desc.get('payload', {})
	if (desc.get('schema') != 'ods.artifact.published' or payload.get('kind') != 'RULES'
		or payload.get('owner') != 'A3' or payload.get('artifact_version') != version
		or payload.get('artifact_id') != 'rules-a3'):
		raise ValueError('Registered RULES descriptor mismatch')
	content = (root / (version + '.json')).read_bytes()
	if hashlib.sha256(content).hexdigest() != payload.get('hash_sha256'):
		raise ValueError('Registered RULES content has changed since registration')
	return validate_s2_rules(json.loads(content))['rules']
