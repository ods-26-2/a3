"""Correlated 6c CMD/RPL exchange over Kafka (JSON only, never video).

Works with an unmodified B3 broker. One temporary reply consumer per request.
The command is published only after a reply-topic assignment is established.
"""
import json
from time import monotonic
from uuid import uuid4

from services.s1_integration import kafka_settings_from_env


def request_reply(envelope, request_topic, reply_topic=None, brokers=None, timeout=8):
	if not brokers:
		raise ValueError('Kafka brokers are required')
	if envelope.get('message_type') not in ('command', 'query') or not envelope.get('target'):
		raise ValueError('Expected a 6c command/query with target')
	if not isinstance(envelope.get('message_id'), str):
		raise ValueError('Missing request message_id')
	from confluent_kafka import Consumer, Producer
	reply_topic = reply_topic or (request_topic + '.reply')
	settings = {'bootstrap.servers': brokers, **kafka_settings_from_env()}
	consumer = Consumer({**settings, 'group.id': f'a3-rpc-{uuid4()}',
		'auto.offset.reset': 'latest', 'enable.auto.commit': False})
	producer = Producer(settings)
	deadline = monotonic() + float(timeout)
	try:
		consumer.subscribe([reply_topic])
		while not consumer.assignment() and monotonic() < deadline:
			consumer.poll(0.15)
		if not consumer.assignment():
			raise TimeoutError(f'No consumer assignment for {reply_topic}')
		producer.produce(request_topic, json.dumps(envelope, ensure_ascii=False).encode('utf-8'))
		if producer.flush(max(0.01, min(3, deadline - monotonic()))) != 0:
			raise TimeoutError(f'Kafka did not confirm publication on {request_topic}')
		while monotonic() < deadline:
			msg = consumer.poll(min(0.5, max(0.01, deadline - monotonic())))
			if msg is None:
				continue
			if msg.error():
				raise RuntimeError(str(msg.error()))
			try:
				reply = json.loads(msg.value())
			except (TypeError, ValueError, UnicodeDecodeError):
				continue
			if (not isinstance(reply, dict) or reply.get('schema') != reply_topic
					or reply.get('correlation_id') != envelope['message_id']
					or reply.get('producer') != envelope['target']):
				continue
			if reply.get('message_type') != 'reply' or not isinstance(reply.get('payload'), dict):
				continue
			return reply['payload']
		raise TimeoutError(f'No correlated {reply_topic} reply')
	finally:
		consumer.close()
