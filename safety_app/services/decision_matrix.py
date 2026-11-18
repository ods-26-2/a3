"""A3 deterministic, auditable risk decision; never a substitute for S2 qualifying events.

S1 owns geometry and occupancy, S2 qualifies events, A3 classifies contextual
severity using A3-owned policy, S3 owns alert routing, S4 owns video evidence.
"""
from dataclasses import asdict, dataclass
from math import isfinite

from services.risk_policy import effective_risk, match_rule, load_policy, EVENT_TYPES

SEVERITY = ('NONE', 'LOW', 'MEDIUM', 'HIGH')


@dataclass(frozen=True)
class Decision:
	grade: int
	severity: str
	action: str
	risk_level: str
	matched_rule: str
	policy_version: str
	event_type: str
	object_class: str
	movement: str
	recurrence_count: int
	confidence: float
	reasons: tuple
	status: str

	def to_dict(self):
		result = asdict(self)
		result['reasons'] = list(self.reasons)
		return result


def _number(value):
	if isinstance(value, bool):
		return None
	try:
		value = float(value)
	except (TypeError, ValueError):
		return None
	return value if isfinite(value) else None


def _movement(payload, context):
	if payload.get('event_type') == 'ZONE_ENTER':
		return 'ENTER'
	if payload.get('event_type') == 'ZONE_EXIT':
		return 'EXIT'
	if payload.get('event_type') == 'OBJECT_STATIONARY':
		return 'STATIONARY'
	details = payload.get('details') or {}
	if not isinstance(details, dict):
		details = {}
	for value in (details.get('movement'), details.get('transition'), context.get('movement')):
		if isinstance(value, str) and value.upper() in ('ENTER', 'EXIT', 'STATIONARY', 'MOVING'):
			return value.upper()
	return 'UNKNOWN'


def evaluate_event(payload, recurrence_count=0, policy=None, context=None):
	"""Evaluate the S2-qualified event without generating an event_id or S2 event.

	Unknown zones, invalid/out-of-catalog event types and uncertain observations
	require review rather than an invented classification. Grades are local A3
	business decisions, not S2 status and not an S3 delivery acknowledgment.
	"""
	if not isinstance(payload, dict):
		raise ValueError('Invalid S2 event payload')
	policy = load_policy() if policy is None else policy
	context = context or {}
	if type(recurrence_count) is not int or recurrence_count < 0:
		raise ValueError('Invalid recurrence count')
	status, phase = payload.get('status'), payload.get('phase')
	if status not in ('CONFIRMED', 'UNCERTAIN') or phase not in ('STARTED', 'UPDATED', 'ENDED'):
		raise ValueError('S2 status/phase outside contract 6c')
	confidence = _number(payload.get('confidence'))
	if confidence is None or not 0 <= confidence <= 1:
		raise ValueError('Invalid S2 confidence')
	event_type = payload.get('event_type')
	object_class = str(payload.get('class') or context.get('class') or 'unknown').strip().lower()
	movement = _movement(payload, context)
	zone_id = payload.get('zone_id')
	risk = effective_risk(policy, zone_id, payload.get('started_at')) if zone_id else None

	def result(grade, action, rule_id, reasons):
		return Decision(grade=grade, severity=SEVERITY[grade], action=action,
			risk_level=risk or 'UNCONFIGURED', matched_rule=rule_id,
			policy_version=policy['version'], event_type=event_type or '',
			object_class=object_class, movement=movement,
			recurrence_count=recurrence_count, confidence=confidence,
			reasons=tuple(reasons), status=status)

	if phase == 'ENDED':
		return result(0, 'MONITOR', 'a3-ended', ['Evento encerrado; não emitir novo alerta.'])
	if status == 'UNCERTAIN':
		return result(0, 'REVIEW', 'a3-uncertain', ['S2: UNCERTAIN; exigir revisão humana.'])
	if event_type not in EVENT_TYPES:
		return result(0, 'REVIEW', 'a3-unknown-event', ['Tipo de evento não configurado.'])
	if risk is None:
		return result(0, 'REVIEW', 'a3-unconfigured-zone', [f'Zona {zone_id or "ausente"}: risco não configurado no A3.'])
	min_confidence = policy.get('min_decision_confidence', 0.0)
	if confidence < min_confidence:
		return result(0, 'REVIEW', 'a3-low-confidence',
			[f'Confiança {confidence:.3f} inferior ao mínimo A3 {min_confidence:.3f}.'])
	rule = match_rule(policy, risk, event_type, object_class, movement)
	if rule is None:
		return result(0, 'REVIEW', 'a3-no-rule', ['Nenhuma regra corresponde ao contexto.'])
	grade, action = rule['grade'], rule['action']
	reasons = [f'Regra {rule["rule_id"]}: {risk}/{event_type}/{object_class}/{movement}.']
	if grade > 0 and recurrence_count >= policy['recurrence_escalation_after']:
		grade = min(3, grade + 1)
		action = 'ALERT' if grade >= 2 else action
		reasons.append(f'Reincidência: {recurrence_count} eventos anteriores na janela.')
	details = payload.get('details') or {}
	if not isinstance(details, dict):
		details = {}
	if event_type == 'DWELL_EXCEEDED':
		dwell = _number(details.get('dwell_s'))
		limit = _number(details.get('max_dwell_s', details.get('threshold')))
		if dwell is None and _number(payload.get('duration_ms')) is not None:
			dwell = _number(payload['duration_ms']) / 1000
		if limit is None and _number(details.get('min_duration_ms')) is not None:
			limit = _number(details['min_duration_ms']) / 1000
		if dwell is not None and limit is not None and limit > 0:
			reasons.append(f'Permanência {dwell:g}s, limite {limit:g}s.')
			if grade > 0 and dwell >= 2 * limit:
				grade = min(3, grade + 1)
				action = 'ALERT' if grade >= 2 else action
				reasons.append('Permanência >= 2x o limite: escalonamento A3.')
	if event_type == 'OCCUPANCY_ABOVE':
		count = _number(details.get('count', details.get('occupancy')))
		limit = _number(details.get('max_occupancy', details.get('threshold')))
		if count is not None and limit is not None and limit > 0:
			reasons.append(f'Ocupação {count:g}, capacidade {limit:g}.')
			if grade > 0 and count >= 2 * limit:
				grade = min(3, grade + 1)
				action = 'ALERT' if grade >= 2 else action
				reasons.append('Ocupação >= 2x a capacidade: escalonamento A3.')
	return result(grade, action, rule['rule_id'], reasons)
