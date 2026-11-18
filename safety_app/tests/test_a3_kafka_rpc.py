"""Broker-free transport simulation for 6c correlated request/reply (no media)."""
import json
import sys
import types
import unittest
from unittest.mock import patch

from services.clips import build_clip_query
from services.kafka_rpc import request_reply


class CorrelatedKafkaRPC(unittest.TestCase):
	def test_matches_only_s4_reply_for_original_message_id(self):
		requests = []
		queue = []

		class FakeMessage:
			def __init__(self, content):
				self.content = content
			def error(self):
				return None
			def value(self):
				return json.dumps(self.content).encode('utf-8')

		class FakeConsumer:
			def __init__(self, opts):
				self.opts = opts
			def subscribe(self, topics):
				self.topics = topics
			def assignment(self):
				return [1]
			def poll(self, seconds):
				return queue.pop(0) if queue else None
			def close(self):
				pass

		class FakeProducer:
			def __init__(self, opts):
				self.opts = opts
			def produce(self, topic, value):
				request = json.loads(value)
				requests.append((topic, request))
				queue.extend([
					FakeMessage({'schema': topic + '.reply', 'producer': 'S4',
						'message_type': 'reply', 'correlation_id': 'wrong-id',
						'payload': {'status': 'OK', 'clips': ['wrong']}}),
					FakeMessage({'schema': topic + '.reply', 'producer': 'S4',
						'message_type': 'reply', 'correlation_id': request['message_id'],
						'payload': {'status': 'OK', 'clips': [{'clip_id': 'test-clip'}]}}),
				])
			def flush(self, timeout):
				return 0

		fake_module = types.SimpleNamespace(Consumer=FakeConsumer, Producer=FakeProducer)
		with patch.dict(sys.modules, {'confluent_kafka': fake_module}):
			cmd = build_clip_query(event_id='abc-event-id')
			result = request_reply(cmd, 'ods.s4.clip.query', brokers='broker:9092', timeout=0.5)
		self.assertEqual(result['clips'][0]['clip_id'], 'test-clip')
		self.assertEqual(requests[0][0], 'ods.s4.clip.query')
		self.assertEqual(requests[0][1]['target'], 'S4')
		self.assertNotIn('mp4', json.dumps(requests[0][1]).lower())


if __name__ == '__main__':
	unittest.main()
