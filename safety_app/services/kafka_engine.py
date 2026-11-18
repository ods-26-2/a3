"""A3 Kafka adapter: consume S2 contracts, classify using A3 policy, and audit.

Live mode is opt-in and never sends media. S3 commands are opt-in separately;
S3 may also route S2 events automatically by ROUTING, so duplicate alert
commands must be excluded through the deployment configuration (default OFF).
"""
import json
import logging
import os
import threading
from datetime import datetime, timezone
from dataclasses import replace
from uuid import UUID, uuid5
from pathlib import Path

import pandas as pd

from services.decision_matrix import Decision, evaluate_event
from services.event_history import EventHistory
from services.kafka_rpc import request_reply
from services.kafka_config import get_kafka_bootstrap_servers
from services.risk_policy import load_policy
from services.s1_integration import kafka_settings_from_env
from services.alert_delivery import resolve_delivery
from services.s3_alerts import S3AlertStore, ALERT_EVENT, valid_uuid

LOGGER = logging.getLogger(__name__)
S2_EVENT_TOPIC = 'ods.s2.event'
S2_ENTITY_TOPIC = 'ods.s2.entity'
S3_ALERT_COMMAND_TOPIC = 'ods.s3.alert.command'
DISPLAY_COLUMNS = ['ID do evento', 'Grau de violação', 'Nome da câmera', 'Horário da violação', 'Tipo']


def build_s3_alert_command(event_payload, decision, recipients=None, channels=None, delivery_policy=None):
	"""Build an S3 command only for an actual A3 alert decision.

	Stable UUID5 by event and grade aids idempotency when a response is lost;
	S3 is still responsible for enforcing its own message_id deduplication.
	"""
	if decision.action != 'ALERT' or decision.grade < 1 or decision.status != 'CONFIRMED':
		raise ValueError('A3 decision does not require S3 alert')
	event_id = event_payload.get('event_id')
	try:
		UUID(str(event_id))
	except (ValueError, TypeError, AttributeError) as exc:
		raise ValueError('S3 alert command requires an S2 UUID event_id') from exc
	route, _delivery_version = resolve_delivery(decision, policy=delivery_policy)
	zone = event_payload.get('zone_id') or 'sem zona'
	entity = event_payload.get('entity_id') or 'não informada'
	when = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
	return {'message_type': 'command',
		'message_id': str(uuid5(UUID(event_id), f'A3-S3-GRADE-{decision.grade}')),
		'schema': S3_ALERT_COMMAND_TOPIC, 'schema_version': '1.0',
		'producer': 'A3', 'target': 'S3', 'published_at': when,
		'payload': {
			'title': f'Risco A3 · {decision.event_type}',
			'message': f'Entidade {entity}; zona {zone}; regra A3 {decision.matched_rule}.',
			'severity': decision.severity,
			'recipients': recipients if recipients is not None else route['recipients'],
			'channels': channels if channels is not None else route['channels'],
			'event_id': event_id,
		}}


class A3KafkaEngine:
	"""Optional live S2 consumer with durable, deduplicated A3 event history."""

	def __init__(self, brokers=None, publish_alerts=False, max_events=500, history_path=None, s3_store_path=None,
		alert_sender=None, policy_provider=None, delivery_policy_provider=None):
		self.brokers = (brokers if brokers is not None else get_kafka_bootstrap_servers()).strip()
		self.publish_alerts = bool(publish_alerts)
		# Optional dependency injection for strictly offline contract simulations.
		# Defaults remain the persisted A3 policy and real Kafka request/reply.
		self.alert_sender = alert_sender
		self.policy_provider = policy_provider or load_policy
		self.delivery_policy_provider = delivery_policy_provider
		self.max_events = int(max_events)
		if self.max_events <= 0:
			raise ValueError('max_events must be positive')
		self.history = EventHistory(history_path)
		if s3_store_path is None and history_path is not None:
			s3_store_path = str(Path(history_path).with_name('s3_' + Path(history_path).name))
		self.s3_store = S3AlertStore(path=s3_store_path)
		self._lock = threading.RLock()
		self._entities = {}
		self._last_error = None
		self._stop = threading.Event()
		self._thread = None

	@property
	def last_error(self):
		with self._lock:
			return self._last_error

	def _record_error(self, error):
		LOGGER.warning('A3 Kafka: %s', error)
		with self._lock:
			self._last_error = str(error)

	def start(self):
		if not self.brokers:
			raise RuntimeError('Set A3_KAFKA_BOOTSTRAP_SERVERS for real S2 events')
		with self._lock:
			if self._thread is not None and self._thread.is_alive():
				return
			self._stop.clear()
			self._thread = threading.Thread(target=self._consume_loop, name='a3-s2-consumer', daemon=True)
			self._thread.start()

	def stop(self):
		self._stop.set()
		thread = self._thread
		if thread is not None and thread.is_alive():
			thread.join(timeout=3)

	def process_message(self, envelope):
		"""Consume real/fixture 6c envelopes; no broker needed to unit-test."""
		if not isinstance(envelope, dict):
			return None
		if envelope.get('schema') == ALERT_EVENT:
			try:
				return self.s3_store.record_event(envelope)
			except (ValueError, TypeError, KeyError) as exc:
				self._record_error(f'Invalid S3 alert event: {exc}')
				return None
		if envelope.get('producer') != 'S2':
			return None
		schema, payload = envelope.get('schema'), envelope.get('payload')
		if envelope.get('message_type') != 'event' or not isinstance(payload, dict):
			return None
		if schema == S2_ENTITY_TOPIC:
			entities = payload.get('entities', [])
			if not isinstance(entities, list):
				return None
			with self._lock:
				for entity in entities:
					if isinstance(entity, dict) and entity.get('entity_id'):
						self._entities[entity['entity_id']] = {
							'class': entity.get('class'),
							'track_refs': entity.get('track_refs', []),
						}
			return None
		if schema != S2_EVENT_TOPIC:
			return None
		event_id = payload.get('event_id')
		try:
			UUID(str(event_id))
		except (ValueError, TypeError, AttributeError):
			return None
		phase, status = payload.get('phase'), payload.get('status')
		if phase not in ('STARTED', 'UPDATED', 'ENDED') or status not in ('CONFIRMED', 'UNCERTAIN'):
			self._record_error(f'Invalid S2 status/phase for {event_id}')
			return None
		with self._lock:
			old = self.history.get(event_id)
			if old and old['phase'] == 'ENDED':
				return Decision(**{**old['decision'], 'reasons': tuple(old['decision']['reasons'])})
			if old and phase == 'STARTED':
				# STARTED replay after the first accepted event is not a new incident.
				return Decision(**{**old['decision'], 'reasons': tuple(old['decision']['reasons'])})
			if old and phase == 'UPDATED' and isinstance(payload.get('duration_ms'), int):
				if old.get('duration_ms') is not None and payload['duration_ms'] < old['duration_ms']:
					return Decision(**{**old['decision'], 'reasons': tuple(old['decision']['reasons'])})
			if old and phase == 'ENDED':
				# Preserve the peak classification for historical audit, but stop
				# presenting an ended incident as an active alert request.
				previous = Decision(**{**old['decision'], 'reasons': tuple(old['decision']['reasons'])})
				decision = replace(previous, status=status, action='MONITOR',
					reasons=previous.reasons + ('S2 phase ENDED: incident closed.',))
				self.history.upsert(envelope, decision, old['alerted_grade'], old['attempted_grade'])
				return decision
			policy = self.policy_provider()
			eid = payload.get('entity_id')
			context = self._entities.get(eid, {}) if eid else {}
			recurrence = (old['decision']['recurrence_count'] if old else
				self.history.recurrence_count(eid, payload.get('started_at'),
					policy['recurrence_window_hours'], event_id))
			decision = evaluate_event(payload, recurrence_count=recurrence, policy=policy, context=context)
			if (old and status == 'CONFIRMED' and decision.status == 'CONFIRMED'
				and decision.action != 'REVIEW' and old['decision']['grade'] > decision.grade):
				# An UPDATED confirmation must not erase a previously higher
				# risk grade. Never override UNCERTAIN/REVIEW in this way.
				previous = Decision(**{**old['decision'], 'reasons': tuple(old['decision']['reasons'])})
				decision = replace(previous, confidence=decision.confidence,
					reasons=previous.reasons + ('Peak grade preserved on UPDATED.',))
			self.history.upsert(envelope, decision,
				old['alerted_grade'] if old else 0,
				old['attempted_grade'] if old else 0)
			should_send = (self.publish_alerts and decision.status == 'CONFIRMED'
				and decision.action == 'ALERT' and decision.grade > 0
				and decision.grade > (old['attempted_grade'] if old else 0))
		if should_send:
			command, registered, reply_received = None, False, False
			try:
				# Validate delivery policy BEFORE declaring an attempted send.
				command = build_s3_alert_command(payload, decision,
						delivery_policy=self.delivery_policy_provider() if self.delivery_policy_provider else None)
				registered = self.s3_store.record_attempt(command, decision.grade)
				self.history.mark_alert_attempted(event_id, decision.grade)
				if not registered:
					return decision  # Already attempted; never resend after timeout.
				reply = self._send_alert(command)
				reply_received = True
				status = self.s3_store.record_reply(command['message_id'], reply=reply)
				if status != 'CONFIRMED':
					self._record_error(f'S3 rejected or did not confirm {event_id}: {reply}')
				else:
					self.history.mark_alert_acknowledged(event_id, decision.grade)
			except Exception as exc:
				if registered and not reply_received:
					self.s3_store.record_reply(command['message_id'], error=exc)
				if registered:
					self._record_error(f'S3 reply unconfirmed for {event_id}; inspect S3 before retry: {exc}')
				else:
					self._record_error(f'S3 command not prepared for {event_id}: {exc}')
		return decision

	def _send_alert(self, envelope):
		if self.alert_sender is not None:
			return self.alert_sender(envelope)
		return request_reply(envelope, S3_ALERT_COMMAND_TOPIC,
			brokers=self.brokers, timeout=float(os.getenv('A3_RPC_TIMEOUT_SEC', '8')))

	def snapshot_violations(self):
		rows = []
		for item in self.history.recent(self.max_events):
			decision = item['decision']
			if decision['grade'] <= 0:
				continue
			sources = item['payload'].get('source_ids') or []
			camera = sources[0] if isinstance(sources, list) and sources else item['source_id'] or '—'
			rows.append({'ID do evento': item['event_id'], 'Grau de violação': decision['grade'],
				'Nome da câmera': camera, 'Horário da violação': item['started_at'],
				'Tipo': item['event_type']})
		df = pd.DataFrame(rows, columns=DISPLAY_COLUMNS)
		if not df.empty:
			df['Horário da violação'] = pd.to_datetime(df['Horário da violação'], utc=True, errors='coerce')
		return df

	def snapshot_decisions(self):
		return {item['event_id']: item['decision'] for item in self.history.recent(self.max_events)}

	def snapshot_all_events(self):
		return self.history.recent(self.max_events)

	def entity_tracks(self, entity_id):
		with self._lock:
			return [x.get('track_id') for x in self._entities.get(entity_id, {}).get('track_refs', [])
				if isinstance(x, dict)]

	def _consume_loop(self):
		consumer = None
		try:
			from confluent_kafka import Consumer
			consumer = Consumer({**kafka_settings_from_env(),
				'bootstrap.servers': self.brokers,
				'group.id': os.getenv('A3_KAFKA_CONSUMER_GROUP', 'a3-qualified-events'),
				'auto.offset.reset': 'earliest', 'enable.auto.commit': True})
			consumer.subscribe([S2_EVENT_TOPIC, S2_ENTITY_TOPIC, ALERT_EVENT])
			while not self._stop.is_set():
				msg = consumer.poll(0.5)
				if msg is None:
					continue
				if msg.error():
					self._record_error(msg.error())
					continue
				try:
					self.process_message(json.loads(msg.value()))
				except (ValueError, TypeError, UnicodeDecodeError, KeyError) as exc:
					self._record_error(f'Invalid S2/S3 envelope: {exc}')
		except Exception as exc:
			self._record_error(exc)
		finally:
			if consumer is not None:
				consumer.close()
