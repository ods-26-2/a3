"""A3: recebe a geometria normalizada do B6 e prepara artefatos para S1.

B6 é uma dependência externa imutável; nenhuma regra de desenho pertence ao A3.
"""
import json
import math
import os
import re
from pathlib import Path


DEFAULT_STORE = Path(__file__).resolve().parents[1] / 'data' / 'zones_b6.json'


def _store_path():
	return Path(os.environ.get('A3_ZONES_PATH', str(DEFAULT_STORE))).expanduser().resolve()


def _polygon_area(points):
	return abs(sum(
		points[i]['x'] * points[(i + 1) % len(points)]['y']
		- points[(i + 1) % len(points)]['x'] * points[i]['y']
		for i in range(len(points))
	)) / 2


def validate_zones(zones):
	"""Validate the public B6 ZonePayload[] contract, without changing B6."""
	if not isinstance(zones, list):
		raise ValueError('B6 zones must be an array')
	cleaned = []
	for index, zone in enumerate(zones):
		if not isinstance(zone, dict):
			raise ValueError(f'Zone {index}: expected object')
		points = zone.get('points')
		if not isinstance(points, list) or len(points) < 3:
			raise ValueError(f'Zone {index}: at least 3 vertices required')
		clean_points = []
		for point in points:
			if not isinstance(point, dict):
				raise ValueError(f'Zone {index}: invalid vertex')
			x, y = point.get('x'), point.get('y')
			if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or not 0 <= v <= 1 for v in (x, y)):
				raise ValueError(f'Zone {index}: vertices must be normalized to [0, 1]')
			clean_points.append({'x': float(x), 'y': float(y)})
		if len({(p['x'], p['y']) for p in clean_points}) < 3 or _polygon_area(clean_points) < 1e-9:
			raise ValueError(f'Zone {index}: degenerate polygon')
		# B6 already rejects self-intersections. A3 validates its input again before publishing.
		if _crosses_itself(clean_points):
			raise ValueError(f'Zone {index}: self-intersecting polygon')
		name = str(zone.get('name') or f'Zone {index + 1}').strip()
		zone_type = str(zone.get('type') or '').strip()
		if not name:
			raise ValueError(f'Zone {index}: name required')
		# B6 generates a new zone_id every export (Date.now). A3 owns durable IDs.
		# The saved ID is stable across exports of the same named zone.
		cleaned.append({'name': name, 'type': zone_type, 'points': clean_points})
	return cleaned


def _crosses_itself(points):
	def orientation(a, b, c):
		return (b['x'] - a['x']) * (c['y'] - a['y']) - (b['y'] - a['y']) * (c['x'] - a['x'])
	def on_segment(a, p, b):
		return min(a['x'], b['x']) - 1e-12 <= p['x'] <= max(a['x'], b['x']) + 1e-12 and min(a['y'], b['y']) - 1e-12 <= p['y'] <= max(a['y'], b['y']) + 1e-12
	def intersects(a, b, c, d):
		o = [orientation(a, b, c), orientation(a, b, d), orientation(c, d, a), orientation(c, d, b)]
		if o[0] * o[1] < 0 and o[2] * o[3] < 0:
			return True
		return any(abs(x) < 1e-12 and on_segment(u, p, v) for x, u, p, v in [
			(o[0], a, c, b), (o[1], a, d, b), (o[2], c, a, d), (o[3], c, b, d)])
	n = len(points)
	return any(intersects(points[i], points[(i + 1) % n], points[j], points[(j + 1) % n])
		for i in range(n) for j in range(i + 1, n)
		if j != i + 1 and not (i == 0 and j == n - 1))


def load_zones():
	path = _store_path()
	if not path.is_file():
		return []
	data = json.loads(path.read_text(encoding='utf-8'))
	if not isinstance(data, dict) or data.get('format') != 'A3_B6_ZONES_1':
		raise ValueError(f'Unexpected A3 zones store format: {path}')
	zones = data.get('zones')
	if not isinstance(zones, list):
		raise ValueError('Invalid A3 zones store')
	for zone in zones:
		if not isinstance(zone, dict) or not re.fullmatch(r'[a-zA-Z0-9_-]+', str(zone.get('zone_id', ''))):
			raise ValueError('Invalid stored zone_id')
	# Validate geometry; preserve identifiers assigned by A3.
	clean = validate_zones(zones)
	return [{**row, 'zone_id': zones[i]['zone_id']} for i, row in enumerate(clean)]


def confirm_zone_config(zones):
	clean = validate_zones(zones)
	previous = load_zones()
	by_name = {z['name']: z['zone_id'] for z in previous}
	used = set()
	result = []
	for index, zone in enumerate(clean):
		base = re.sub(r'[^a-z0-9_-]+', '-', zone['name'].lower()).strip('-') or f'zone-{index + 1}'
		zone_id = by_name.get(zone['name'], f'a3-{base}')
		candidate = zone_id
		counter = 2
		while candidate in used:
			candidate = f'{zone_id}-{counter}'
			counter += 1
		used.add(candidate)
		result.append({'zone_id': candidate, **zone})
	path = _store_path()
	path.parent.mkdir(parents=True, exist_ok=True)
	tmp = path.with_suffix('.tmp')
	tmp.write_text(json.dumps({'format': 'A3_B6_ZONES_1', 'zones': result}, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
	tmp.replace(path)
	return len(result)


def to_s1_pixels(zones, source_id, image_width, image_height, enter_hysteresis_ms=500, exit_hysteresis_ms=1500,
		classes=None, max_occupancy=None):
	"""Produce the architecture 6c ZONES artifact; NO risk policy goes to S1."""
	if not isinstance(source_id, str) or not source_id.strip():
		raise ValueError('source_id required')
	for key, value, minimum in [('image_width', image_width, 1), ('image_height', image_height, 1),
			('enter_hysteresis_ms', enter_hysteresis_ms, 0), ('exit_hysteresis_ms', exit_hysteresis_ms, 0)]:
		if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
			raise ValueError(f'{key} must be an integer >= {minimum}')
	if max_occupancy is not None and (isinstance(max_occupancy, bool) or not isinstance(max_occupancy, int) or max_occupancy < 1):
		raise ValueError('max_occupancy must be >= 1')
	if classes is not None and (not isinstance(classes, list) or not all(isinstance(c, str) and c.strip() for c in classes)):
		raise ValueError('classes must be a list of non-empty strings')
	clean = validate_zones(zones)
	if len(clean) != len(zones):
		raise ValueError('Invalid zones')
	if not zones:
		raise ValueError('At least one zone must be configured')
	converted = []
	ids = set()
	for index, zone in enumerate(zones):
		zone_id = zone.get('zone_id')
		if not isinstance(zone_id, str) or not re.fullmatch(r'[a-zA-Z0-9_-]+', zone_id) or zone_id in ids:
			raise ValueError(f'Invalid or duplicated zone_id at {index}')
		ids.add(zone_id)
		p = clean[index]
		item = {'zone_id': zone_id, 'name': p['name'],
			'polygon': [[round(pt['x'] * image_width), round(pt['y'] * image_height)] for pt in p['points']],
			'enter_hysteresis_ms': enter_hysteresis_ms, 'exit_hysteresis_ms': exit_hysteresis_ms}
		if len(set(map(tuple, item['polygon']))) < 3:
			raise ValueError(f'Zone {zone_id}: polygon collapses at selected resolution')
		if classes:
			item['classes'] = list(classes)
		if max_occupancy is not None:
			item['max_occupancy'] = max_occupancy
		converted.append(item)
	return {'coordinates': 'PIXELS', 'source_ids': [source_id.strip()], 'zones': converted}
