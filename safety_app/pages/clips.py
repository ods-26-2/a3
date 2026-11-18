"""A3-only clip view. Mock and real S4 query paths are strictly separated."""
import streamlit as st
from services.clips import query_s4, playable_url
from services.mock_validation import get_mock_clips, get_mock_video_data_uri, mock_event_ids
from components.b6_video_player import render_b6_video_player


def render_clips_page():
	st.title('Clipes de eventos')
	mock_mode = st.toggle('Usar dados simulados do A3 (sem Kafka)', value=True, key='a3_mock_clips_enabled')
	if mock_mode:
		st.caption('Consulta S4 simulada localmente. Nenhuma mensagem é enviada ao Kafka; vídeo sintético é reproduzido fora do barramento.')
	else:
		st.caption('Consulta real S4 por Kafka; mídia sempre recuperada fora do barramento.')

	incoming_id = st.session_state.get('a3_clip_event_id', '')
	if st.session_state.get('a3_last_incoming_clip_id') != incoming_id:
		st.session_state['a3_event_lookup'] = incoming_id
		st.session_state['a3_last_incoming_clip_id'] = incoming_id
		st.session_state.pop('a3_s4_clips', None)
		st.session_state.pop('a3_clips_for', None)
		st.session_state.pop('a3_clips_mode', None)
	if 'a3_event_lookup' not in st.session_state:
		st.session_state['a3_event_lookup'] = incoming_id
	event_id = st.text_input('ID do evento', key='a3_event_lookup').strip()
	if mock_mode:
		st.caption('IDs de teste: ' + ', '.join(e[:12] + '…' for e in sorted(mock_event_ids())))
	# Dashboard selection is an explicit request in both mock and live modes.
	auto_lookup = bool(st.session_state.pop('a3_clip_auto_lookup', False))
	query_clicked = st.button('Consultar clipes', type='primary')

	if query_clicked or auto_lookup:
		st.session_state.pop('a3_s4_clips', None)
		st.session_state['a3_clips_for'] = event_id
		st.session_state['a3_clips_mode'] = 'mock' if mock_mode else 'real'
		if not event_id:
			st.warning('Selecione um evento ou informe um event_id.')
		else:
			try:
				if mock_mode:
					sim = st.session_state.get('a3_integrated_simulator')
					if sim is not None and sim.owns_event(event_id):
						st.session_state['a3_s4_clips'] = sim.query_clips(event_id)
					else:
						st.session_state['a3_s4_clips'] = get_mock_clips(event_id)
				else:
					with st.spinner('Consultando S4 pelo Kafka...'):
						st.session_state['a3_s4_clips'] = query_s4(event_id=event_id)
			except Exception as exc:
				st.error(f'Falha na consulta: {exc}')

	if (st.session_state.get('a3_clips_for') != event_id or
		st.session_state.get('a3_clips_mode') != ('mock' if mock_mode else 'real')):
		st.info('Consulte os clipes do identificador informado.')
		return

	clips = st.session_state.get('a3_s4_clips')
	if clips is None:
		st.info('Informe o ID de um evento e consulte os clipes.')
		return
	if not clips:
		st.warning('Nenhum clipe associado a este evento (ou mídia já expirada).')
		return

	labels = [f"{item.get('clip_id', 'sem ID')[:12]}… · {item.get('media_type', 'CLIP')}" for item in clips]
	index = st.selectbox('Clipes encontrados', range(len(clips)), format_func=lambda i: labels[i],
		key=f"a3_clip_choice_{'mock' if mock_mode else 'real'}_{event_id}")
	clip = clips[index]
	st.json(clip, expanded=False)
	if mock_mode:
		st.info('Player B6 com vídeo sintético local. O s4:// nos metadados é apenas uma referência fictícia de validação.')
		render_b6_video_player(get_mock_video_data_uri())
		return

	url = playable_url(clip.get('media_uri'))
	if not url:
		st.warning('S4 retornou metadados, mas não uma URL HTTP(S) de vídeo acessível. A implementação atual do S4 não oferece entrega HTTP do MP4.')
		return
	render_b6_video_player(url)
