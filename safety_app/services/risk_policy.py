"""A3-owned, versioned and auditable zone semantics and decision matrix.

No risk classification, display-only labels, or business actions are sent to S1.
The policy is a local A3 artifact; only RULES (when explicitly enabled) are
sent to S2 through the documented command contract.
"""
import json
import hashlib
import os
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

POLICY_FORMAT = 'A3_RISK_POLICY_1'
LEVELS = ('SAFE', 'ATTENTION', 'PROHIBITED')
EVENT_TYPES = ('ZONE_ENTER', 'ZONE_EXIT', 'DWELL_EXCEEDED', 'OCCUPANCY_ABOVE',
	'OCCUPANCY_BELOW', 'PPE_NON_COMPLIANT', 'OBJECT_STATIONARY', 'CROSS_CAMERA_MATCH')
SEVERITIES = ('NONE', 'LOW', 'MEDIUM', 'HIGH')
ACTIONS = ('MONITOR', 'REGISTER', 'REVIEW', 'ALERT')
DEFAULT_PATH = Path(__file__).resolve().parents[1] / 'data' / 'risk_policy.json'
# Baseline decision matrix. All numeric values and overrides belong to the A3
# application; these are editable example defaults, NOT official client policy.
BASE_GRADES = {
	'SAFE':       (0, 0, 0, 0, 0, 1, 0, 0),
	'ATTENTION':  (1, 0, 2, 2, 0, 2, 1, 0),
	'PROHIBITED': (3, 0, 3, 3, 0, 3, 3, 0),
}


def default_policy():
	rules = []
	for level in LEVELS:
		for event_type, grade in zip(EVENT_TYPES, BASE_GRADES[level]):
			rules.append({'rule_id': f'a3-{level.lower()}-{event_type.lower()}',
				'risk_level': level, 'event_type': event_type, 'object_class': '*',
				'movement': '*', 'grade': grade,
				'action': ('MONITOR' if grade == 0 else 'REGISTER' if grade == 1 else 'ALERT')})
	# Explicit object/movement-specific override demonstrating matrix precedence.
	rules.append({'rule_id': 'a3-attention-vehicle-entry', 'risk_level': 'ATTENTION',
		'event_type': 'ZONE_ENTER', 'object_class': 'vehicle', 'movement': 'ENTER',
		'grade': 2, 'action': 'ALERT'})
	return {'format': POLICY_FORMAT, 'version': 'a3-risk-1',
		'timezone': 'America/Sao_Paulo', 'recurrence_window_hours': 24,
		'recurrence_escalation_after': 2, 'min_decision_confidence': 0.0, 'zones': {}, 'rules': rules}


def _path():
	return Path(os.getenv('A3_RISK_POLICY_PATH', str(DEFAULT_PATH))).expanduser().resolve()


def validate_policy(policy):
	if not isinstance(policy, dict) or policy.get('format') != POLICY_FORMAT:
		raise ValueError('Unexpected A3 risk policy format')
	version = policy.get('version')
	if not isinstance(version, str) or not re.fullmatch(r'[a-zA-Z0-9_.-]+', version):
		raise ValueError('Invalid policy version')
	try:
		ZoneInfo(policy.get('timezone', 'America/Sao_Paulo'))
	except (KeyError, TypeError, ValueError) as exc:
		raise ValueError('Invalid timezone') from exc
	if type(policy.get('recurrence_window_hours')) is not int or not 1 <= policy['recurrence_window_hours'] <= 720:
		raise ValueError('recurrence_window_hours must be 1..720')
	if type(policy.get('recurrence_escalation_after')) is not int or not 1 <= policy['recurrence_escalation_after'] <= 100:
		raise ValueError('recurrence_escalation_after must be 1..100')
	minimum = policy.get('min_decision_confidence', 0.0)
	if isinstance(minimum, bool) or not isinstance(minimum, (int, float)) or not 0 <= minimum <= 1:
		raise ValueError('min_decision_confidence must be 0..1')
	zones = policy.get('zones')
	if not isinstance(zones, dict):
		raise ValueError('zones must be an object')
	for zone_id, config in zones.items():
		if not isinstance(zone_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]+', zone_id):
			raise ValueError('Invalid zone id')
		if not isinstance(config, dict) or config.get('risk_level') not in LEVELS:
			raise ValueError(f'Invalid risk level for zone {zone_id}')
		if not isinstance(config.get('schedule', []), list):
			raise ValueError('Zone schedule must be a list')
		for schedule in config.get('schedule', []):
			if not isinstance(schedule, dict) or schedule.get('risk_level') not in LEVELS:
				raise ValueError(f'Invalid schedule for {zone_id}')
			if not isinstance(schedule.get('weekdays'), list) or any(type(d) is not int or d not in range(7) for d in schedule['weekdays']):
				raise ValueError('weekdays must be [0..6] (Monday=0)')
			for key in ('start', 'end'):
				if not isinstance(schedule.get(key), str) or not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d', schedule[key]):
					raise ValueError(f'Invalid schedule {key}')
	if not isinstance(policy.get('rules'), list) or not policy['rules']:
		raise ValueError('A3 requires at least one decision rule')
	ids = set()
	for rule in policy['rules']:
		if not isinstance(rule, dict) or not isinstance(rule.get('rule_id'), str) or not rule['rule_id']:
			raise ValueError('Invalid rule_id')
		if rule['rule_id'] in ids:
			raise ValueError(f'Duplicate rule: {rule["rule_id"]}')
		ids.add(rule['rule_id'])
		if rule.get('risk_level') not in LEVELS or rule.get('event_type') not in EVENT_TYPES:
			raise ValueError(f'Invalid risk/event in {rule["rule_id"]}')
		if not isinstance(rule.get('object_class'), str) or not rule['object_class']:
			raise ValueError('Invalid object class')
		if rule.get('movement') not in ('*', 'ENTER', 'EXIT', 'STATIONARY', 'MOVING', 'UNKNOWN'):
			raise ValueError('Invalid movement')
		if type(rule.get('grade')) is not int or rule['grade'] not in range(4):
			raise ValueError('Grade must be 0..3')
		if rule.get('action') not in ACTIONS:
			raise ValueError('Invalid action')
		if rule['grade'] == 0 and rule['action'] == 'ALERT':
			raise ValueError('Cannot alert on grade 0')
	# Reject rule pairs that would be tied on specificity for some context.
	rules = policy['rules']
	for i, a in enumerate(rules):
		for b in rules[i + 1:]:
			if a['risk_level'] != b['risk_level'] or a['event_type'] != b['event_type']:
				continue
			pa = int(a['object_class'] != '*') + int(a['movement'] != '*')
			pb = int(b['object_class'] != '*') + int(b['movement'] != '*')
			if (pa == pb and (a['object_class'] == b['object_class'] or '*' in (a['object_class'], b['object_class']))
				and (a['movement'] == b['movement'] or '*' in (a['movement'], b['movement']))):
				raise ValueError(f'Ambiguous A3 rules: {a["rule_id"]} and {b["rule_id"]}')
	return deepcopy(policy)


def load_policy():
	path = _path()
	if not path.exists():
		return default_policy()
	return validate_policy(json.loads(path.read_text(encoding='utf-8')))


def save_policy(policy):
	"""Save an A3 policy and immutable versioned archive; reject version reuse."""
	result = validate_policy(policy)
	path = _path()
	path.parent.mkdir(parents=True, exist_ok=True)
	if path.is_file():
		old = validate_policy(json.loads(path.read_text(encoding='utf-8')))
		if old['version'] == result['version'] and old != result:
			raise ValueError('Editions require a NEW policy version for auditability')
	elif result != default_policy() and result['version'] == default_policy()['version']:
		raise ValueError('Change the version when editing the baseline policy')
	data = json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + '\n'
	archive = path.parent / 'risk_policy_versions' / (result['version'] + '.json')
	archive.parent.mkdir(parents=True, exist_ok=True)
	if archive.exists() and archive.read_text(encoding='utf-8') != data:
		raise ValueError('Cannot overwrite an archived A3 policy version')
	if not archive.exists():
		with archive.open('x', encoding='utf-8') as fh:
			fh.write(data)
	tmp = path.with_suffix('.tmp')
	tmp.write_text(data, encoding='utf-8')
	tmp.replace(path)
	return str(path)


def set_zone_risk(zone_id, risk_level, policy=None):
	if risk_level not in LEVELS:
		raise ValueError('Invalid risk level')
	if not re.fullmatch(r'[A-Za-z0-9_-]+', zone_id):
		raise ValueError('Invalid zone id')
	policy = deepcopy(policy if policy is not None else load_policy())
	old = policy['zones'].get(zone_id, {})
	policy['zones'][zone_id] = {**old, 'risk_level': risk_level}
	return validate_policy(policy)


def _clock(s):
	h, m = map(int, s.split(':'))
	return h * 60 + m


def effective_risk(policy, zone_id, at=None):
	"""Resolve A3 zone semantics. No fallback to B6 zone.type or S2 metadata."""
	config = policy['zones'].get(zone_id)
	if not config:
		return None
	if at is None:
		at = datetime.now(timezone.utc)
	elif isinstance(at, str):
		try:
			at = datetime.fromisoformat(at.replace('Z', '+00:00'))
		except ValueError:
			return None
	if at.tzinfo is None:
		return None
	local = at.astimezone(ZoneInfo(policy['timezone']))
	minute = local.hour * 60 + local.minute
	for period in config.get('schedule', []):
		start, end = _clock(period['start']), _clock(period['end'])
		if start == end:
			matched = local.weekday() in period['weekdays']
		elif start < end:
			matched = local.weekday() in period['weekdays'] and start <= minute < end
		else:
			# Overnight windows are attached to their starting weekday.
			matched = ((local.weekday() in period['weekdays'] and minute >= start) or
				((local.weekday() - 1) % 7 in period['weekdays'] and minute < end))
		if matched:
			return period['risk_level']
	return config['risk_level']


def match_rule(policy, risk_level, event_type, object_class, movement):
	matches = []
	for rule in policy['rules']:
		if rule['risk_level'] != risk_level or rule['event_type'] != event_type:
			continue
		if rule['object_class'] not in ('*', object_class) or rule['movement'] not in ('*', movement):
			continue
		priority = int(rule['object_class'] != '*') + int(rule['movement'] != '*')
		matches.append((priority, rule))
	if not matches:
		return None
	# Equal-specificity ties are invalid policy design; detect instead of guessing.
	max_priority = max(p for p, _ in matches)
	best = [r for p, r in matches if p == max_priority]
	if len(best) > 1:
		raise ValueError('Ambiguous A3 decision rules: ' + ', '.join(r['rule_id'] for r in best))
	return best[0]


def demo_policy():
	"""Self-contained test fixture: never writes or changes production policy."""
	policy = default_policy()
	policy['version'] = 'a3-demo-risk-1'
	policy['zones'] = {
		'a3-zona-restrita-1': {'risk_level': 'PROHIBITED'},
		'a3-zona-armazenamento': {'risk_level': 'ATTENTION'},
		'a3-zona-carga': {'risk_level': 'ATTENTION'},
		'a3-zona-espera': {'risk_level': 'SAFE'},
		'a3-zona-acesso': {'risk_level': 'SAFE'},
	}
	return validate_policy(policy)


def policy_for_saved_zones(policy, zones):
	"""Return the unclassified B6/S1 zones without silently assigning risk."""
	return [{'zone_id': zone['zone_id'], 'name': zone['name'],
		'risk_level': policy['zones'].get(zone['zone_id'], {}).get('risk_level', 'UNCONFIGURED')}
		for zone in zones]
