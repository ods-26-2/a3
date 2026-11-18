import unittest
from unittest.mock import patch
from services.clips import build_clip_query, playable_url, query_s4


class S4ClipsTest(unittest.TestCase):
	def test_query_envelope(self):
		message = build_clip_query(event_id='event-123')
		self.assertEqual(message['schema'], 'ods.s4.clip.query')
		self.assertEqual(message['payload'], {'event_id': 'event-123'})
		self.assertEqual(message['message_type'], 'query')
		self.assertEqual(message['producer'], 'A3')

	def test_reject_ambiguous_query(self):
		with self.assertRaises(ValueError):
			build_clip_query()
		with self.assertRaises(ValueError):
			build_clip_query(event_id='a', clip_id='b')

	def test_media_uri_requires_real_http_endpoint(self):
		self.assertIsNone(playable_url('s4://clips/a.mp4'))
		self.assertIsNone(playable_url('/var/s4/media/a.mp4'))
		self.assertEqual(playable_url('https://media.example.org/a.mp4'), 'https://media.example.org/a.mp4')

	def test_explicit_empty_broker_override(self):
		# Explicitly empty override still prevents accidental S4 network access.
		with patch.dict('os.environ', {'A3_KAFKA_BOOTSTRAP_SERVERS': ''}):
			with self.assertRaises(RuntimeError):
				query_s4(event_id='a')


if __name__ == '__main__':
	unittest.main()
