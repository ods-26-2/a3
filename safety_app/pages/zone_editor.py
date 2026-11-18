"""A3 screen integrating B6.2's original ZoneEditor API and S1 6c artifact flow."""
import json
import os
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from services.zones import confirm_zone_config, load_zones
from services.kafka_config import get_kafka_bootstrap_servers
from services.s1_integration import (
	SCHEMA, build_artifact, kafka_settings_from_env, publish_artifact, send_announcement,
)


_FRONTEND = Path(__file__).resolve().parents[1] / 'components' / 'b6_zone_editor_frontend'
_b6_editor = components.declare_component('a3_b6_zone_editor', path=str(_FRONTEND))


def render_zone_editor_page():
	st.subheader('Editor de zonas — B6.2')
	st.caption('O editor B6 é um artefato original, sem alterações. O backend A3 valida, persiste e publica o ZONES 6c.')
	value = _b6_editor(key='a3_zone_editor')
	if isinstance(value, dict) and 'submitted_at' in value:
		request_id = value['submitted_at']
		if st.session_state.get('a3_last_b6_request') != request_id:
			try:
				count = confirm_zone_config(value.get('zones', []))
				st.session_state['a3_last_b6_request'] = request_id
				st.success(f'{count} zona(s) registrada(s) no A3.')
			except (ValueError, OSError, json.JSONDecodeError) as error:
				st.error(f'Falha na validação das zonas B6: {error}')
	st.info('B6: clique esquerdo adiciona vértices; clique direito em área vazia fecha o polígono. '
		'O botão de confirmação exporta apenas zonas FECHADAS. O B6 atual não oferece API pública para restaurar desenhos após recarga.')
	try:
		zones = load_zones()
	except (ValueError, OSError, json.JSONDecodeError) as error:
		st.error(f'Erro ao consultar zonas salvas: {error}')
		return
	st.caption(f'Zonas confirmadas no A3: {len(zones)}')
	with st.expander('Consultar geometria salva'):
		st.json(zones)
	with st.expander('Publicar artefato ZONES para a instância S1 desta aplicação', expanded=True):
		st.caption('É necessário que A3 e S1 compartilhem o MESMO registro de arquivos, '
			'e que o S1 assine ods.artifact.published no mesmo broker. Publicação no Kafka não comprova aplicação pelo S1.')
		registry = st.text_input('Diretório de registro compartilhado (A3)',
			value=os.environ.get('A3_S1_REGISTRY_DIR', ''),
			help='Mesmo volume que S1_REGISTRY_DIR; os caminhos internos podem ser diferentes.')
		artifact_id = st.text_input('Artifact ID da instância S1', value=os.environ.get('A3_S1_ARTIFACT_ID', 'zones-a3'),
			help='Deve coincidir com S1_ZONES_ARTIFACT_ID.')
		camera = st.text_input('Câmera: source_id', value=os.environ.get('A3_SOURCE_ID', 'cam0'))
		cols = st.columns(2)
		with cols[0]:
			width = st.number_input('Largura real do quadro (pixels)', value=1920, min_value=1, step=1)
			enter = st.number_input('Histerese de entrada (ms)', value=500, min_value=0, step=100)
		with cols[1]:
			height = st.number_input('Altura real do quadro (pixels)', value=1080, min_value=1, step=1)
			exit_ms = st.number_input('Histerese de saída (ms)', value=1500, min_value=0, step=100)
		classes_raw = st.text_input('Classes permitidas (separadas por vírgula; vazio = todas)', value='person')
		max_occ = st.number_input('Ocupação máxima (0 = campo ausente)', value=0, min_value=0, step=1)
		broker = st.text_input('Kafka bootstrap.servers', value=get_kafka_bootstrap_servers(),
			help='Broker público informado: 200.17.66.56:9092 (porta alternativa: 9093). Envio manual.')
		topic = st.text_input('Tópico do anúncio (padrão 6c)', value=os.environ.get('A3_ARTIFACT_TOPIC', SCHEMA),
			help='S1_TOPIC_ARTIFACT_PUBLISHED deve ter o mesmo nome, caso seja personalizado.')
		publish = st.checkbox('Enviar anúncio Kafka após registrar o artefato', value=False,
			help='Desabilitado por padrão para não consumir créditos da turma sem autorização explícita.')
		if st.button('Registrar versão ZONES e, se habilitado, publicar no Kafka', type='primary', disabled=not zones):
			try:
				if not registry.strip():
					raise ValueError('Informe o diretório compartilhado do registro')
				if publish and not broker.strip():
					raise ValueError('Informe o broker Kafka para enviar o anúncio')
				classes = [c.strip() for c in classes_raw.split(',') if c.strip()] or None
				artifact = build_artifact(camera, int(width), int(height), int(enter), int(exit_ms), classes,
					int(max_occ) if max_occ else None)
				result = publish_artifact(artifact, registry, artifact_id.strip(),
					bootstrap_servers=broker.strip() if publish else None,
					topic=topic.strip(), kafka_options=kafka_settings_from_env())
				st.session_state['a3_last_publish'] = result
				st.success(f'Artefato {result["version"]} registrado em {result["path"]}.')
				if result['announced']:
					st.success('Broker Kafka confirmou a entrega. S1 ainda precisa consumir e aplicar a versão.')
				else:
					st.warning('Somente registro realizado: nenhum anúncio enviado ao S1.')
			except (ValueError, OSError, RuntimeError) as error:
				st.error(f'Falha na publicação: {error}. Se uma versão foi criada, reutilize-a no campo de reenvio.')
		last = st.session_state.get('a3_last_publish')
		if last:
			st.json(last['message'])
		st.markdown('**Reenviar versão registrada (sem alterar a geometria)**')
		retry = st.text_input('Versão existente, por exemplo zones-a3-1', key='a3_retry_version')
		if st.button('Reenviar anúncio via Kafka'):
			try:
				message = send_announcement(registry, retry.strip(), broker.strip(), topic=topic.strip(),
					kafka_options=kafka_settings_from_env())
				st.success(f'Broker confirmou o anúncio de {message["payload"]["artifact_version"]}. '
					'O S1 pode aplicar a versão na próxima sessão.')
			except (ValueError, OSError, RuntimeError, json.JSONDecodeError) as error:
				st.error(f'Reenvio falhou: {error}')
