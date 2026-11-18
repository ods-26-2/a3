"""A3-owned ROUTING artifact content for S3's documented automatic alerts.

It does not create alerts or route to users itself. Manual S3 commands from
A3 must be disabled when ROUTING already creates equivalent alerts.
"""
from copy import deepcopy
from services.risk_policy import EVENT_TYPES


def validate_routing(data):
	if not isinstance(data, dict) or not isinstance(data.get('routes'), list) or not data['routes']:
		raise ValueError('ROUTING requires a nonempty routes array')
	for route in data['routes']:
		if not isinstance(route, dict) or route.get('event_type') not in EVENT_TYPES:
			raise ValueError('Invalid S3 route event_type')
		if route.get('min_status') not in ('CONFIRMED', 'UNCERTAIN'):
			raise ValueError('Invalid S3 route min_status')
		if route.get('severity') not in ('LOW', 'MEDIUM', 'HIGH'):
			raise ValueError('Invalid S3 route severity')
		for k in ('recipients', 'channels'):
			if not isinstance(route.get(k), list) or not route[k] or not all(isinstance(x, str) and x for x in route[k]):
				raise ValueError(f'Invalid S3 route {k}')
		if not all(c in ('SCREEN', 'SOUND', 'PUSH') for c in route['channels']):
			raise ValueError('Unknown S3 channel')
		for k in ('title', 'message'):
			if not isinstance(route.get(k), str) or not route[k].strip():
				raise ValueError(f'Missing S3 route {k}')
		if type(route.get('capture')) is not bool:
			raise ValueError('S3 route capture must be boolean')
	return deepcopy(data)
