"""A3-owned S2 RULES: explicit qualification contract, separate from A3 risk matrix.

S2 decides WHICH qualified event exists. A3 decides severity and action AFTER
receiving S2's canonical event_id. Never invent an S2 event here.
"""
import json
import re
from math import isfinite
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from services.kafka_rpc import request_reply
from services.risk_policy import EVENT_TYPES

DRAFT_PATH = Path(__file__).resolve().parents[1] / 'data' / 's2_qualification_rules.json'
SCHEMA = 'ods.s2.rules.command'


def validate_s2_rules(data):
	"""Validate fields exposed in 6c; do not assume unpublished S2 extensions."""
	if not isinstance(data, dict) or not isinstance(data.get('rules'), list) or not data['rules']:
		raise ValueError('S2 requires a nonempty rules array')
	rules = data['rules']
	seen = set()
	for rule in rules:
		if not isinstance(rule, dict):
			raise ValueError('S2 rule must be an object')
		rid = rule.get('rule_id')
		if not isinstance(rid, str) or not re.fullmatch(r'[a-zA-Z0-9_-]+', rid) or rid in seen:
			raise ValueError('Invalid or duplicated S2 rule_id')
		seen.add(rid)
		if rule.get('event_type') not in EVENT_TYPES:
			raise ValueError(f'Unknown S2 event_type for {rid}')
		# Both confidence thresholds are required in the public S2 RULES contract.
		for key in ('min_confidence', 'uncertain_below'):
			value = rule.get(key)
			if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or not 0 <= value <= 1:
				raise ValueError(f'Invalid or missing {key} for {rid}')
		if rule['uncertain_below'] < rule['min_confidence']:
			raise ValueError(f'uncertain_below < min_confidence for {rid}')
		for key in ('min_duration_ms', 'debounce_ms'):
			if key in rule and (type(rule[key]) is not int or rule[key] < 0):
				raise ValueError(f'Invalid {key} for {rid}')
		if rule.get('event_type') in ('DWELL_EXCEEDED', 'OCCUPANCY_ABOVE', 'OCCUPANCY_BELOW', 'OBJECT_STATIONARY'):
			if (isinstance(rule.get('threshold'), bool) or not isinstance(rule.get('threshold'), (int, float))
				or not isfinite(rule['threshold']) or rule['threshold'] <= 0):
				raise ValueError(f'S2 {rid} requires a positive threshold')
		if rule.get('event_type') == 'PPE_NON_COMPLIANT' and not rule.get('equipment_item'):
			raise ValueError(f'S2 {rid} requires equipment_item')
	return deepcopy({'rules': rules})


def load_draft():
	if not DRAFT_PATH.exists():
		return {'rules': []}
	return json.loads(DRAFT_PATH.read_text(encoding='utf-8'))


def save_draft(data):
	validated = validate_s2_rules(data)
	DRAFT_PATH.parent.mkdir(parents=True, exist_ok=True)
	DRAFT_PATH.write_text(json.dumps(validated, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
	return validated


def build_s2_rules_command(rules, artifact_version):
	validated = validate_s2_rules({'rules': rules})
	if not isinstance(artifact_version, str) or not re.fullmatch(r'rules-a3-[1-9][0-9]*', artifact_version):
		raise ValueError('Expected artifact_version rules-a3-N')
	return {'message_type': 'command', 'message_id': str(uuid4()),
		'schema': SCHEMA, 'schema_version': '1.0',
		'producer': 'A3', 'target': 'S2',
		'published_at': datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
		'payload': {'artifact_version': artifact_version, **validated}}


def send_s2_rules(rules, artifact_version, brokers):
	cmd = build_s2_rules_command(rules, artifact_version)
	reply = request_reply(cmd, SCHEMA, brokers=brokers, timeout=10)
	if reply.get('status') != 'OK':
		raise RuntimeError(f'S2 rejected RULES: {reply}')
	return {'command': cmd, 'reply': reply}
