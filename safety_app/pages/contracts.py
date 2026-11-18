"""Explicit operator-driven A3 publication of S2 RULES and S3 ROUTING.

No Kafka activity on page load; no changes to S2/S3 or any external source.
"""
import json
import os

import streamlit as st

from services.qualification_rules import (
	load_draft, save_draft, send_s2_rules, validate_s2_rules,
)
from services.routing_policy import validate_routing
from services.shared_artifacts import register_artifact, publish_registered, load_registered_rules
from services.kafka_config import get_kafka_bootstrap_servers


def render_contracts_page():
	st.title('Regras de qualificação e roteamento · A3')
	st.info('S2 é dono da qualificação e do event_id. A3 configura RULES e classifica os eventos recebidos. A3 define destinatários/canais nos comandos contextuais; S3 encaminha e mantém alertas. ROUTING automático é um caminho alternativo. Publicação real somente por comando explícito.')
	registry = st.text_input('Registro compartilhado para RULES/ROUTING (S2/S3)',
		value=os.getenv('A3_SHARED_REGISTRY_DIR', ''),
		help='Mesmo conteúdo acessível a S2/S3; registrar localmente não implica carregamento remoto.')
	broker = st.text_input('Broker Kafka para S2/S3', value=get_kafka_bootstrap_servers(),
		help='Broker Jetson da UFSJ: acessível somente na rede interna. A publicação permanece manual.')
	with st.expander('RULES: definir quais condições o S2 qualifica', expanded=True):
		st.caption('A3 mantém sua MATRIZ DE RISCO separada dessas regras de qualificação. Não envie graus de risco ao S2.')
		example = {'rules': [
			{'rule_id': 'a3-entry-example', 'event_type': 'ZONE_ENTER', 'class': 'person',
				'zone_id': 'SUBSTITUA-PELA-ZONA-REAL', 'min_confidence': 0.7,
				'uncertain_below': 0.85, 'min_duration_ms': 500, 'debounce_ms': 500}]}
		draft = load_draft()
		rules_text = st.text_area('Conteúdo JSON de RULES (revise zonas e limites)',
			value=json.dumps(draft if draft['rules'] else example, ensure_ascii=False, indent=2), height=260)
		c1, c2 = st.columns(2)
		with c1:
			if st.button('Validar e salvar rascunho RULES', key='a3_save_s2_rules'):
				try:
					save_draft(json.loads(rules_text))
					st.success('Rascunho de qualificação A3 validado e salvo localmente.')
				except (ValueError, TypeError, OSError, json.JSONDecodeError) as exc:
					st.error(str(exc))
		with c2:
			if st.button('Registrar versão imutável RULES', key='a3_register_s2_rules'):
				try:
					result = register_artifact('RULES', json.loads(rules_text), registry)
					st.session_state['a3_last_rules_artifact'] = result['version']
					st.success(f"RULES {result['version']} registrado em {result['path']}")
				except (ValueError, TypeError, OSError, KeyError, json.JSONDecodeError) as exc:
					st.error(str(exc))
		version = st.text_input('Versão RULES registrada',
			value=st.session_state.get('a3_last_rules_artifact', ''), key='a3_rules_version')
		st.caption('O comando utiliza SOMENTE o conteúdo da versão registrada, mesmo se o editor acima tiver alterações não registradas.')
		if st.button('Enviar comando ods.s2.rules.command (após registro)', key='a3_send_rules'):
			try:
				if not broker.strip():
					raise ValueError('Configure Kafka para envio real')
				result = send_s2_rules(load_registered_rules(registry, version.strip()),
					version.strip(), broker.strip())
				st.success('S2 respondeu OK ao comando RULES.')
				st.json(result['reply'])
			except (ValueError, TypeError, OSError, RuntimeError, TimeoutError, json.JSONDecodeError) as exc:
					st.error(f'Não foi possível confirmar aceitação de RULES no S2: {exc}')
	with st.expander('ROUTING: definir como o S3 transforma eventos em alertas', expanded=False):
		st.caption('ROUTING permite alertas automáticos S2→S3 e NÃO deve ser ativado para os mesmos eventos tratados por comandos A3→S3. Para a matriz contextual, prefira configurar destinatários/canais na página Alertas S3.')
		routing_example = {'routes': [
			{'event_type': 'ZONE_ENTER', 'min_status': 'CONFIRMED',
				'severity': 'HIGH', 'recipients': ['role:supervisor'],
				'channels': ['SCREEN'], 'title': 'Entrada em zona proibida',
				'message': 'Entidade {entity_id} entrou na zona {zone_id}', 'capture': True}]}
		routing_text = st.text_area('Conteúdo JSON ROUTING',
			value=json.dumps(routing_example, ensure_ascii=False, indent=2), height=260)
		if st.button('Validar e registrar versão imutável ROUTING', key='a3_register_routing'):
			try:
				result = register_artifact('ROUTING', validate_routing(json.loads(routing_text)), registry)
				st.session_state['a3_last_routing_artifact'] = result['version']
				st.success(f"ROUTING {result['version']} registrado em {result['path']}")
			except (ValueError, TypeError, OSError, KeyError, json.JSONDecodeError) as exc:
					st.error(str(exc))
	st.divider()
	st.subheader('Anunciar um artefato já registrado (ods.artifact.published)')
	version = st.text_input('Versão imutável (rules-a3-N ou routing-a3-N)', key='a3_artifact_version_announce')
	st.caption('Broker confirma publicação; isso não comprova que S2/S3 carregaram a versão. Registro/identificador e política de carregamento devem estar alinhados à instância.')
	if st.button('Publicar anúncio explicitamente', key='a3_publish_general_artifact'):
		try:
			message = publish_registered(registry, version.strip(), broker.strip())
			st.success(f'Kafka confirmou anúncio {message["payload"]["artifact_version"]}.')
		except (ValueError, TypeError, OSError, RuntimeError, TimeoutError, KeyError, json.JSONDecodeError) as exc:
			st.error(f'Falha ao publicar: {exc}')
