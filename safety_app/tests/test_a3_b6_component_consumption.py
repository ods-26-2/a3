"""A3-only consumer checks for immutable upstream B6 and S3 action contract."""
import hashlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from services.b6_alert_adapter import to_b6_alert, to_b6_alerts
from services.s3_alerts import (
	ALERT_ACTION, S3AlertStore, S3Client, MockS3Client,
	build_alert_action_command, _mock_envelope,
)


ROOT = Path(__file__).resolve().parents[1]


class B6AdapterContracts(unittest.TestCase):
	def test_s3_to_b6_severities_and_states(self):
		for s3, b6 in [('HIGH', 'CRITICAL'), ('MEDIUM', 'WARNING'), ('LOW', 'INFO')]:
			for state, expected in [('CREATED', 'ACTIVE'), ('ACKNOWLEDGED', 'ACKNOWLEDGED'),
				('MUTED', 'MUTED'), ('CLOSED', 'CLOSED')]:
				with self.subTest(severity=s3, state=state):
					payload = to_b6_alert({'alert_id':'alert-x', 'message':'Evento',
						'severity':s3, 'state':state, 'created_at':'2026-11-01T12:00:00.000Z'})
					self.assertEqual(payload['severity'], b6)
					self.assertEqual(payload['status'], expected)
					self.assertEqual(payload['timestamp'], 1793534400000)

	def test_b6_message_is_escaped_without_modifying_b6(self):
		data = to_b6_alert({'alert_id':'alert-x', 'message':'<img src=x onerror=alert(1)>',
			'severity':'HIGH', 'state':'CREATED', 'created_at':'2026-11-01T12:00:00Z'})
		self.assertEqual(data['message'], escape('<img src=x onerror=alert(1)>', quote=True))
		self.assertNotIn('<img', data['message'])

	def test_invalid_s3_payload_does_not_fake_b6_values(self):
		base = {'alert_id':'alert-x', 'message':'Evento', 'severity':'HIGH',
			'state':'CREATED', 'created_at':'2026-11-01T12:00:00Z'}
		for bad in [{'severity':'CRITICAL'}, {'state':'ACTIVE'},
			{'created_at':'2026-11-01T12:00:00'}, {'created_at':'garbage'}, {'alert_id':None}]:
			with self.subTest(bad=bad), self.assertRaises(ValueError):
				to_b6_alert({**base, **bad})
		self.assertEqual(to_b6_alerts([]), [])

	def test_compiled_b6_modules_are_immutable_vendor_builds(self):
		# SHA256 from original B6 TypeScript compiled with tsc ES2020,DOM ES2020.
		expected = {
			'components/b6_zone_editor_frontend/b6_zone_editor.js': '63b0091e223a98e4d1327c6baa8b02dfcf5efbb14a3cdb0472e8a2fe7a945ce8',
			'components/b6_video_player_frontend/b6_overlay_player.js': '41fe3c4cb007fc52cfa424e278ed62a5515a9bd49ab8e59c8688b7ce9a860901',
			'components/b6_alert_console_frontend/b6_alert_console.js': '4a7d75ad8fc5bacd5629f2825955f4058c310c83400c451062abae3611a1f210',
		}
		for name, sha in expected.items():
			with self.subTest(name=name):
				self.assertEqual(hashlib.sha256((ROOT/name).read_bytes()).hexdigest(), sha)

	def test_bridge_uses_public_api_only(self):
		zone = (ROOT/'components/b6_zone_editor_frontend/bridge.js').read_text()
		self.assertIn('editor.exportAllZones(', zone)
		self.assertIn('editor.startDrawingMode()', zone)
		player = (ROOT/'components/b6_video_player_frontend/bridge.js').read_text()
		self.assertIn('player.loadVideo(', player)
		self.assertIn('player.loadMetadataTimeline(', player)
		alerts = (ROOT/'components/b6_alert_console_frontend/bridge.js').read_text()
		self.assertIn('new AlertConsole(', alerts)
		self.assertIn('consoleView.loadAlerts(', alerts)
		self.assertIn('onFocusTimeline:', alerts)
		self.assertIn('onAcknowledgeAll:', alerts)


class B6AlertActionsThroughS3(unittest.TestCase):
	def setUp(self):
		self.temporary = tempfile.TemporaryDirectory()
		self.store = S3AlertStore(path=str(Path(self.temporary.name)/'projection.sqlite3'))
		self.mock = MockS3Client(store=self.store)
		self.alert_id = str(uuid4())
		self.when = '2026-11-01T12:00:00Z'
		self.payload = {'alert_id':self.alert_id,'event_id':str(uuid4()),
			'title':'Intrusão','message':'Zona restrita', 'severity':'HIGH',
			'recipients':['role:supervisor'], 'channels':['SCREEN'],
			'state':'CREATED', 'created_at':self.when}
		self.store.record_event(_mock_envelope(self.payload, self.when))

	def tearDown(self):
		self.temporary.cleanup()

	def test_action_envelope_matches_s3_contract(self):
		command = build_alert_action_command(self.alert_id, 'ACKNOWLEDGE', 'user:operator')
		self.assertEqual(command['schema'], ALERT_ACTION)
		self.assertEqual(command['target'], 'S3')
		self.assertEqual(command['payload'], {'alert_id':self.alert_id, 'action':'ACKNOWLEDGE', 'actor':'user:operator'})
		self.assertNotIn('state', command['payload'])
		self.assertNotIn('routing', command['payload'])

	def test_mute_requires_future_utc_and_actor(self):
		later = (datetime.now(timezone.utc)+timedelta(minutes=15)).isoformat()
		cmd = build_alert_action_command(self.alert_id, 'MUTE', 'user:operator', mute_until=later)
		self.assertEqual(cmd['payload']['mute_until'], later)
		for args in [
			(self.alert_id, 'MUTE', 'user:operator', None),
			(self.alert_id, 'MUTE', 'user:operator', self.when),
			(self.alert_id, 'ACKNOWLEDGE', 'user:operator', later),
			(self.alert_id, 'INVALID', 'user:operator', None),
			(self.alert_id, 'CLOSE', 'role:operator', None),
		]:
			with self.subTest(args=args), self.assertRaises(ValueError):
				build_alert_action_command(*args[:3], mute_until=args[3])

	def test_mock_s3_acknowledges_then_closes_via_contract(self):
		ack = self.mock.action(build_alert_action_command(self.alert_id, 'ACKNOWLEDGE', 'user:operator'))
		self.assertEqual(ack, {'status':'OK','alert_id':self.alert_id,'state':'ACKNOWLEDGED'})
		self.assertEqual(self.store.list_alerts()[0]['acknowledged_by'], 'user:operator')
		close = self.mock.action(build_alert_action_command(self.alert_id, 'CLOSE', 'user:operator'))
		self.assertEqual(close['state'], 'CLOSED')
		self.assertEqual(self.store.list_alerts()[0]['closed_by'], 'user:operator')
		self.assertEqual(self.mock.action(build_alert_action_command(self.alert_id, 'CLOSE', 'user:operator'))['error']['code'], 'INVALID_TRANSITION')

	def test_real_s3_client_never_fabricates_state_after_reply(self):
		client = S3Client('broker:9092', store=self.store)
		cmd = build_alert_action_command(self.alert_id, 'ACKNOWLEDGE', 'user:operator')
		with patch('services.s3_alerts.request_reply', return_value={'status':'OK',
			'alert_id':self.alert_id,'state':'ACKNOWLEDGED'}) as rpc:
			self.assertEqual(client.action(cmd)['status'], 'OK')
			self.assertEqual(rpc.call_args.args[1], ALERT_ACTION)
		self.assertEqual(self.store.list_alerts()[0]['state'], 'CREATED')

	def test_real_s3_action_errors_and_timeouts_are_not_confirmations(self):
		client = S3Client('broker:9092', store=self.store)
		cmd = build_alert_action_command(self.alert_id, 'CLOSE', 'user:operator')
		for reply in [{'status':'ERROR', 'error':{'code':'INVALID_TRANSITION'}},
			{'status':'OK','alert_id':str(uuid4()), 'state':'CLOSED'}]:
			with patch('services.s3_alerts.request_reply', return_value=reply), self.assertRaises(RuntimeError):
				client.action(cmd)
		with patch('services.s3_alerts.request_reply', side_effect=TimeoutError('no S3 reply')), self.assertRaises(TimeoutError):
			client.action(cmd)
		self.assertEqual(self.store.list_alerts()[0]['state'], 'CREATED')


if __name__ == '__main__':
	unittest.main()
