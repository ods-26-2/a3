"""A3 delivery settings for explicit S3 alert commands.

This is NOT the 6c ROUTING artifact consumed by S3 for automatic routing.
The policy only supplies recipients/channels after the A3 risk matrix decides.
"""
import json
import os
import re
from copy import deepcopy
from pathlib import Path

DEFAULT_PATH = Path(__file__).resolve().parents[1] / 'data' / 'alert_delivery_policy.json'
CHANNELS = ('SCREEN', 'SOUND', 'PUSH')
_RECIPIENT = re.compile(r'^(?:user|role|group):[A-Za-z0-9_.:-]+$')


def default_delivery_policy():
	"""Demonstration defaults: an administrator must approve real recipients."""
	return {'version': 'a3-delivery-demo-1', 'routes': [
		{'grade': 1, 'recipients': ['role:supervisor'], 'channels': ['SCREEN']},
		{'grade': 2, 'recipients': ['role:supervisor'], 'channels': ['SCREEN']},
		{'grade': 3, 'recipients': ['role:supervisor'], 'channels': ['SCREEN']},
	]}


def validate_delivery_policy(data):
	if not isinstance(data, dict) or not isinstance(data.get('version'), str) or not data['version'].strip():
		raise ValueError('A3 alert delivery policy requires a nonempty version')
	routes = data.get('routes')
	if not isinstance(routes, list) or len(routes) != 3:
		raise ValueError('A3 alert delivery requires one route per grade 1, 2 and 3')
	seen = set()
	for route in routes:
		if not isinstance(route, dict) or type(route.get('grade')) is not int or route['grade'] not in (1, 2, 3):
			raise ValueError('A3 delivery grade must be an integer 1..3')
		if route['grade'] in seen:
			raise ValueError('Duplicate A3 delivery grade')
		seen.add(route['grade'])
		for key in ('recipients', 'channels'):
			value = route.get(key)
			if not isinstance(value, list) or not value or not all(isinstance(s, str) and s.strip() for s in value):
				raise ValueError(f'A3 delivery {key} requires a nonempty string array')
			if len(value) != len(set(value)):
				raise ValueError(f'A3 delivery {key} contains duplicates')
		if not all(_RECIPIENT.fullmatch(s) for s in route['recipients']):
			raise ValueError('Recipients must use user:<id>, group:<id> or role:<id>')
		if not all(s in CHANNELS for s in route['channels']):
			raise ValueError('Channels must be SCREEN, SOUND or PUSH')
	if seen != {1, 2, 3}:
		raise ValueError('Routes for grades 1, 2 and 3 are required')
	return deepcopy(data)


def delivery_path(path=None):
	return Path(path or os.getenv('A3_ALERT_DELIVERY_PATH', str(DEFAULT_PATH))).expanduser().resolve()


def load_delivery_policy(path=None):
	location = delivery_path(path)
	if not location.is_file():
		return validate_delivery_policy(default_delivery_policy())
	return validate_delivery_policy(json.loads(location.read_text(encoding='utf-8')))


def save_delivery_policy(data, path=None):
	policy = validate_delivery_policy(data)
	location = delivery_path(path)
	location.parent.mkdir(parents=True, exist_ok=True)
	if location.exists():
		old = load_delivery_policy(location)
		if old['version'] == policy['version'] and old != policy:
			raise ValueError('Change the policy version when modifying routing rules')
	from tempfile import NamedTemporaryFile
	with NamedTemporaryFile('w', encoding='utf-8', dir=location.parent, delete=False) as tmp:
		json.dump(policy, tmp, ensure_ascii=False, indent=2)
		tmp.write('\n')
		tmp_name = tmp.name
	os.replace(tmp_name, location)
	return location


def resolve_delivery(decision, policy=None):
	policy = validate_delivery_policy(policy) if policy is not None else load_delivery_policy()
	if decision.action != 'ALERT' or decision.status != 'CONFIRMED' or decision.grade not in (1, 2, 3):
		raise ValueError('A3 matrix did not authorize an alert')
	route = next(r for r in policy['routes'] if r['grade'] == decision.grade)
	return deepcopy(route), policy['version']
