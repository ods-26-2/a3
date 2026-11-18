"""A3 client for S4 clip descriptors; Kafka transports JSON only, never media."""
import json
from services.kafka_config import get_kafka_bootstrap_servers
from datetime import datetime, timezone
from uuid import uuid4

QUERY_TOPIC = 'ods.s4.clip.query'
REPLY_TOPIC = 'ods.s4.clip.query.reply'


def build_clip_query(event_id=None, clip_id=None):
	if bool(event_id) == bool(clip_id):
		raise ValueError('Provide exactly one event_id or clip_id')
	return {
		'message_type': 'query', 'message_id': str(uuid4()),
		'schema': QUERY_TOPIC, 'target': 'S4', 'schema_version': '1.0', 'producer': 'A3',
		'published_at': datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
		'payload': {'event_id': event_id} if event_id else {'clip_id': clip_id},
	}


def query_s4(event_id=None, clip_id=None, brokers=None, timeout=8):
	"""Consult real S4 over correlated B3 Kafka JSON, never transfer MP4 bytes."""
	from services.kafka_rpc import request_reply
	brokers = brokers or get_kafka_bootstrap_servers()
	if not brokers:
		raise RuntimeError('Configure A3_KAFKA_BOOTSTRAP_SERVERS to query S4')
	request = build_clip_query(event_id=event_id, clip_id=clip_id)
	payload = request_reply(request, QUERY_TOPIC, REPLY_TOPIC, brokers=brokers, timeout=timeout)
	if payload.get('status') != 'OK':
		raise RuntimeError(str(payload.get('error', 'S4 query failed')))
	clips = payload.get('clips', [])
	if not isinstance(clips, list):
		raise ValueError('Malformed S4 clip descriptor array')
	return clips


def playable_url(media_uri):
	"""Do not invent an HTTP endpoint for an S4 filesystem or s4:// URI."""
	if isinstance(media_uri, str) and media_uri.startswith(('https://', 'http://')):
		return media_uri
	return None
