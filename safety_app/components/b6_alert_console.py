"""A3 adapter to the unmodified B6.3 AlertConsole JavaScript component."""
from pathlib import Path

import streamlit.components.v1 as components

from services.b6_alert_adapter import to_b6_alerts


_FRONTEND = Path(__file__).resolve().parent / 'b6_alert_console_frontend'
_console = components.declare_component('a3_b6_alert_console', path=str(_FRONTEND))


def render_b6_alert_console(alerts):
	"""Return a public B6 callback: kind, alert_id, timestamp and request_id."""
	return _console(alerts=to_b6_alerts(alerts), key='a3_b6_alert_console')
