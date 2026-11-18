"""Build or verify the unmodified B6.1/B6.2/B6.3 public TypeScript modules.

Usage (from safety_app):
    python scripts/sync_b6_assets.py /path/to/b6-main --verify
    python scripts/sync_b6_assets.py /path/to/b6-main

Compiles upstream B6 TS into temporary files. Only A3 component assets can be
updated; NEVER writes into or patches the upstream B6 checkout.
"""
import argparse
import shutil
import subprocess
import tempfile
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[1]
MODULES = {
	'b6-zone-editor/b6_zone_editor.ts': 'components/b6_zone_editor_frontend/b6_zone_editor.js',
	'b6-overlay-player/b6_overlay_player.ts': 'components/b6_video_player_frontend/b6_overlay_player.js',
	'b6-alert-console/b6_alert_console.ts': 'components/b6_alert_console_frontend/b6_alert_console.js',
}


def sync_assets(upstream_root, verify=False):
	root = Path(upstream_root).expanduser().resolve()
	if not root.is_dir():
		raise FileNotFoundError(f'B6 source directory does not exist: {root}')
	if not shutil.which('tsc'):
		raise RuntimeError('TypeScript compiler (tsc) must be installed locally')
	with tempfile.TemporaryDirectory(prefix='a3-verify-b6-') as workspace:
		for source, destination in MODULES.items():
			src = root / source
			if not src.is_file():
				raise FileNotFoundError(f'Missing upstream B6 source: {src}')
			out = Path(workspace) / src.stem
			out.mkdir()
			subprocess.run(['tsc', str(src), '--target', 'ES2020', '--module', 'ES2020',
				'--lib', 'ES2020,DOM', '--skipLibCheck', '--outDir', str(out)], check=True)
			built = out / (src.stem + '.js')
			target = APP_ROOT / destination
			if verify:
				if not target.is_file() or target.read_bytes() != built.read_bytes():
					raise ValueError(f'A3 B6 asset differs from current B6 source: {destination}')
				print(f'OK  {source} matches {destination}')
			else:
				target.parent.mkdir(parents=True, exist_ok=True)
				shutil.copyfile(built, target)
				print(f'UPDATED A3 asset {destination} (upstream left untouched)')


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument('upstream_b6', help='Directory containing B6 original modules (b6-main)')
	parser.add_argument('--verify', action='store_true', help='Only compare: never write any assets')
	args = parser.parse_args()
	sync_assets(args.upstream_b6, verify=args.verify)
