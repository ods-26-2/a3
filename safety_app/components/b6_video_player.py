"""A3 Streamlit bridge for unmodified B6.1 OverlayPlayer ES module."""
from pathlib import Path

import streamlit.components.v1 as components

_FRONTEND = Path(__file__).resolve().parent / 'b6_video_player_frontend'
_player = components.declare_component('a3_b6_overlay_player', path=str(_FRONTEND))


def render_b6_video_player(video_url, timeline=None):
	"""Use B6 public loadVideo/loadMetadataTimeline; media never travels over Kafka."""
	_player(video_url=video_url, timeline=timeline or [], key='a3_b6_overlay_player')
