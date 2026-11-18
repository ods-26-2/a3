"""Deterministic offline acceptance checks for A3 mock event/media flow."""
import unittest
from uuid import UUID
from services.mock_validation import (
	get_scenarios, get_mock_violations, get_mock_normal_observations,
	get_mock_clips, get_mock_video_data_uri, mock_event_ids,
)
from services.clips import build_clip_query


class A3MockValidationTest(unittest.TestCase):
	def test_five_scenarios_three_violations_two_normal(self):
		scenarios = get_scenarios()
		self.assertEqual(len(scenarios), 5)
		self.assertEqual(sum(s['case'] == 'VIOLATION' for s in scenarios), 3)
		self.assertEqual(sum(s['case'] == 'NORMAL_OBSERVATION' for s in scenarios), 2)

	def test_s2_contract_envelopes_and_catalog(self):
		scenarios = [s for s in get_scenarios() if s['case'] == 'VIOLATION']
		self.assertEqual({s['envelope']['payload']['event_type'] for s in scenarios},
			{'ZONE_ENTER', 'OCCUPANCY_ABOVE', 'DWELL_EXCEEDED'})
		for case in scenarios:
			msg = case['envelope']
			p = msg['payload']
			self.assertEqual(msg['schema'], 'ods.s2.event')
			self.assertEqual(msg['message_type'], 'event')
			self.assertEqual(msg['producer'], 'S2')
			UUID(msg['message_id'])
			UUID(p['event_id'])
			self.assertEqual(p['status'], 'CONFIRMED')
			self.assertEqual(p['phase'], 'STARTED')
			self.assertEqual(msg['source_id'], p['source_ids'][0])
			self.assertTrue(p['started_at'].endswith('Z'))
			self.assertTrue(0 <= p['confidence'] <= 1)

	def test_two_normal_s1_observations_have_no_s2_event_id(self):
		normal = [s for s in get_scenarios() if s['case'] == 'NORMAL_OBSERVATION']
		for item in normal:
			self.assertEqual(item['envelope']['schema'], 'ods.s1.zone_occupancy')
			self.assertEqual(item['envelope']['producer'], 'S1')
			self.assertNotIn('event_id', item['envelope']['payload'])
			self.assertLessEqual(item['envelope']['payload']['count'],
				item['envelope']['payload']['max_occupancy'])
		self.assertEqual(len(get_mock_normal_observations()), 2)

	def test_dashboard_ids_correspond_to_s2_messages(self):
		rows = get_mock_violations()
		self.assertEqual(len(rows), 3)
		self.assertEqual(set(rows['ID do evento']), mock_event_ids())

	def test_mock_s4_queries_one_two_zero_clips(self):
		ids = [s['envelope']['payload']['event_id'] for s in get_scenarios() if s['case'] == 'VIOLATION']
		self.assertEqual([len(get_mock_clips(e)) for e in ids], [1, 2, 0])
		self.assertEqual(get_mock_clips('unknown'), [])
		for eid in ids:
			self.assertEqual(build_clip_query(event_id=eid)['payload'], {'event_id': eid})
			for clip in get_mock_clips(eid):
				self.assertEqual(clip['event_id'], eid)
				self.assertEqual(clip['media_type'], 'CLIP')
				self.assertTrue(clip['media_uri'].startswith('s4://'))
				UUID(clip['clip_id'])
				self.assertEqual(len(clip['hash_sha256']), 64)

	def test_local_video_is_not_kafka_payload(self):
		uri = get_mock_video_data_uri()
		self.assertTrue(uri.startswith('data:video/mp4;base64,'))
		for case in get_scenarios():
			self.assertNotIn(uri, str(case['envelope']))


if __name__ == '__main__':
	unittest.main()
