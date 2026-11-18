"""A3-owned SQLite projection of qualified S2 events and contextual decisions.

The primary key is the original S2 event_id. Updates/ends never create fake
incidents. Retains the decision and notification-attempt state across reruns.
"""
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

DEFAULT_PATH = Path(__file__).resolve().parents[1] / 'data' / 'event_history.sqlite3'


class EventHistory:
	def __init__(self, path=None):
		self.path = str(Path(path or os.getenv('A3_EVENT_HISTORY_PATH', str(DEFAULT_PATH))).expanduser().resolve())
		Path(self.path).parent.mkdir(parents=True, exist_ok=True)
		with self._conn() as db:
			db.execute('''CREATE TABLE IF NOT EXISTS a3_events (
				event_id TEXT PRIMARY KEY, session_id TEXT, source_id TEXT,
				event_type TEXT, zone_id TEXT, entity_id TEXT, phase TEXT,
				started_at TEXT, updated_at TEXT, duration_ms INTEGER,
				payload_json TEXT NOT NULL, decision_json TEXT NOT NULL,
				alerted_grade INTEGER NOT NULL DEFAULT 0,
				attempted_grade INTEGER NOT NULL DEFAULT 0)''')
			columns = {r[1] for r in db.execute('PRAGMA table_info(a3_events)')}
			for name, dtype in [('duration_ms', 'INTEGER'), ('attempted_grade', 'INTEGER NOT NULL DEFAULT 0')]:
				if name not in columns:
					db.execute(f'ALTER TABLE a3_events ADD COLUMN {name} {dtype}')
			db.execute('CREATE INDEX IF NOT EXISTS idx_a3_events_start ON a3_events(started_at)')
			db.execute('CREATE INDEX IF NOT EXISTS idx_a3_events_entity ON a3_events(entity_id, started_at)')

	@contextmanager
	def _conn(self):
		conn = sqlite3.connect(self.path, timeout=5)
		conn.execute('PRAGMA busy_timeout=5000')
		try:
			with conn:
				yield conn
		finally:
			conn.close()

	def get(self, event_id):
		with self._conn() as db:
			row = db.execute('''SELECT phase, duration_ms, payload_json, decision_json,
				alerted_grade, attempted_grade, started_at
				FROM a3_events WHERE event_id=?''', (event_id,)).fetchone()
		if row is None:
			return None
		return {'phase': row[0], 'duration_ms': row[1], 'payload': json.loads(row[2]),
			'decision': json.loads(row[3]), 'alerted_grade': row[4],
			'attempted_grade': row[5], 'started_at': row[6]}

	def recurrence_count(self, entity_id, started_at, window_hours, exclude_event_id):
		"""Count distinct previous incidents for one S2 entity, not UPDATED/ENDED.

		Counting uses the event timestamp, not processing time, so replayed older
		events do not create artificial recurrences. Database owns deduplication.
		"""
		if not entity_id:
			return 0
		try:
			at = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
			if at.tzinfo is None:
				return 0
		except (TypeError, ValueError, AttributeError):
			return 0
		cutoff = (at - timedelta(hours=window_hours)).isoformat()
		with self._conn() as db:
			return db.execute('''SELECT COUNT(*) FROM a3_events WHERE entity_id=?
				AND event_id<>? AND julianday(started_at)>=julianday(?)
				AND julianday(started_at)<julianday(?)''',
				(entity_id, exclude_event_id, cutoff, at.isoformat())).fetchone()[0]

	def upsert(self, envelope, decision, alerted_grade=0, attempted_grade=0):
		p = envelope['payload']
		with self._conn() as db:
			db.execute('''INSERT INTO a3_events(event_id, session_id, source_id,
				event_type, zone_id, entity_id, phase, started_at, updated_at,
				duration_ms, payload_json, decision_json, alerted_grade, attempted_grade)
				VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
				ON CONFLICT(event_id) DO UPDATE SET
					phase=excluded.phase, updated_at=excluded.updated_at,
					duration_ms=excluded.duration_ms, payload_json=excluded.payload_json,
					decision_json=excluded.decision_json,
					alerted_grade=MAX(a3_events.alerted_grade,excluded.alerted_grade),
					attempted_grade=MAX(a3_events.attempted_grade,excluded.attempted_grade)''',
				(p['event_id'], envelope.get('session_id'), envelope.get('source_id'),
				p.get('event_type'), p.get('zone_id'), p.get('entity_id'),
				p.get('phase'), p.get('started_at'),
				envelope.get('published_at', p.get('started_at')),
				p.get('duration_ms', 0), json.dumps(p, ensure_ascii=False),
				json.dumps(decision.to_dict(), ensure_ascii=False),
				alerted_grade, attempted_grade))

	def mark_alert_attempted(self, event_id, grade):
		with self._conn() as db:
			db.execute('UPDATE a3_events SET attempted_grade=MAX(attempted_grade, ?) WHERE event_id=?',
				(grade, event_id))

	def mark_alert_acknowledged(self, event_id, grade):
		with self._conn() as db:
			db.execute('UPDATE a3_events SET alerted_grade=MAX(alerted_grade, ?) WHERE event_id=?',
				(grade, event_id))

	def recent(self, limit=500):
		with self._conn() as db:
			rows = db.execute('''SELECT event_id, event_type, zone_id, source_id,
				phase, started_at, payload_json, decision_json, alerted_grade,
				attempted_grade FROM a3_events ORDER BY julianday(started_at) DESC LIMIT ?''',
				(limit,)).fetchall()
		return [dict(event_id=r[0], event_type=r[1], zone_id=r[2], source_id=r[3],
			phase=r[4], started_at=r[5], payload=json.loads(r[6]),
			decision=json.loads(r[7]), alerted_grade=r[8], attempted_grade=r[9])
			for r in rows]
