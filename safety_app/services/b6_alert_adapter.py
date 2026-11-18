"""A3 translation between S3's VER-3 DTO and B6.3's public AlertPayload.

S3 is the source of truth; B6 is presentation only. This module deliberately
contains no alert-rendering implementation and can be tested without Streamlit.
"""
from datetime import datetime, timezone
from html import escape

S3_TO_B6_SEVERITY = {'LOW': 'INFO', 'MEDIUM': 'WARNING', 'HIGH': 'CRITICAL'}
S3_TO_B6_STATE = {'CREATED': 'ACTIVE', 'ACKNOWLEDGED': 'ACKNOWLEDGED',
	'MUTED': 'MUTED', 'CLOSED': 'CLOSED'}


def to_b6_alert(alert):
	"""B6 AlertPayload uses timestamp in epoch *milliseconds*, unlike S3 ISO UTC."""
	if not isinstance(alert, dict):
		raise ValueError('S3 alert must be a dictionary')
	try:
		date = datetime.fromisoformat(alert['created_at'].replace('Z', '+00:00'))
		if date.tzinfo is None or date.utcoffset().total_seconds() != 0:
			raise ValueError('S3 created_at must be UTC')
		severity = S3_TO_B6_SEVERITY[alert['severity']]
		status = S3_TO_B6_STATE[alert['state']]
	except (TypeError, KeyError, AttributeError, ValueError, OverflowError) as exc:
		raise ValueError('Invalid S3 alert for B6.3') from exc
	alert_id = alert.get('alert_id')
	if not isinstance(alert_id, str) or not alert_id:
		raise ValueError('Missing S3 alert_id')
	# B6.3 renders `message` with innerHTML. Escape upstream text in the A3 adapter
	# instead of editing B6's source code.
	message = escape(str(alert.get('message') or ''), quote=True)
	return {'id': alert_id, 'severity': severity, 'status': status,
		'message': message, 'timestamp': int(date.timestamp() * 1000)}


def to_b6_alerts(alerts):
	return [to_b6_alert(alert) for alert in alerts]
