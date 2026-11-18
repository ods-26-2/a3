"""A3-local 6c test fixtures; five scenarios (three S2, two normal S1).

Synthetic IDs are stable but NEVER published as real events. The same A3
decision engine classifies mocks; grades are not hard-coded in the UI.
"""
import base64
import json
from pathlib import Path

import pandas as pd

from services.decision_matrix import evaluate_event
from services.risk_policy import demo_policy

DATA = Path(__file__).resolve().parents[1] / 'data'


def get_scenarios():
	return json.loads((DATA / 'mock_scenarios.json').read_text(encoding='utf-8'))['scenarios']


def get_mock_decision(scenario):
	if scenario['case'] != 'VIOLATION':
		return None
	return evaluate_event(scenario['envelope']['payload'], policy=demo_policy())


def get_mock_violations():
	rows = []
	for scenario in get_scenarios():
		if scenario['case'] != 'VIOLATION':
			continue
		event = scenario['envelope']['payload']
		decision = get_mock_decision(scenario)
		rows.append({'ID do evento': event['event_id'],
			'Grau de violação': decision.grade,
			'Nome da câmera': scenario['a3_display']['camera_label'],
			'Horário da violação': pd.Timestamp(event['started_at']),
			'Tipo': scenario['label']})
	return pd.DataFrame(rows).sort_values('Horário da violação', ascending=False).reset_index(drop=True)


def get_mock_normal_observations():
	rows = []
	for scenario in get_scenarios():
		if scenario['case'] != 'NORMAL_OBSERVATION':
			continue
		occupancy = scenario['envelope']['payload']
		rows.append({'Câmera': scenario['a3_display']['camera_label'],
			'Zona': occupancy['zone_id'], 'Ocupação': occupancy['count'],
			'Limite': occupancy.get('max_occupancy'),
			'Horário': pd.Timestamp(scenario['envelope']['captured_at']),
			'Situação': 'Sem violação'})
	return pd.DataFrame(rows)


def get_mock_clips(event_id):
	data = json.loads((DATA / 'mock_clips.json').read_text(encoding='utf-8'))
	return data['clips_by_event_id'].get(event_id, [])


def mock_event_ids():
	return {s['envelope']['payload']['event_id'] for s in get_scenarios() if s['case'] == 'VIOLATION'}


def get_mock_video_data_uri():
	"""Synthetic local MP4, inlined into the browser player. No broker media."""
	return 'data:video/mp4;base64,' + base64.b64encode((DATA / 'a3_demo_video.mp4').read_bytes()).decode('ascii')


def get_mock_matrix_evaluation():
	rows = []
	for scenario in get_scenarios():
		if scenario['case'] != 'VIOLATION':
			continue
		payload = scenario['envelope']['payload']
		decision = get_mock_decision(scenario)
		rows.append({'Evento': scenario['label'],
			'ID do evento': payload['event_id'],
			'Zona': payload.get('zone_id'),
			'Risco da zona': decision.risk_level,
			'Classe': decision.object_class,
			'Movimentação': decision.movement,
			'Confiança': decision.confidence,
			'Grau calculado': decision.grade,
			'Severidade': decision.severity,
			'Ação': decision.action,
			'Regra': decision.matched_rule,
			'Justificativas': '; '.join(decision.reasons)})
	return pd.DataFrame(rows)
