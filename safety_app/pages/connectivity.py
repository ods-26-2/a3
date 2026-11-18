"""Manual, read-only connectivity diagnostics for the public Kafka IP."""
import streamlit as st

from services.connectivity import probe_broker
from services.kafka_config import (PUBLIC_KAFKA_HOST, PUBLIC_KAFKA_PORTS,
	get_kafka_bootstrap_servers)


def render_connectivity_page():
	st.subheader('Teste de conexão — Kafka público')
	st.caption('Diagnóstico manual. Não consome eventos, não publica mensagens e não requer S1/S2/S3/S4 operacionais.')
	st.info('Endereço público informado: 200.17.66.56, portas 9092 e 9093. '
		'O endereço privado anterior (172.18.4.56) continua disponível apenas via configuração alternativa.')
	st.write(f'**Broker padrão do A3:** `{get_kafka_bootstrap_servers() or "desabilitado"}`')
	host = st.text_input('IP/host para diagnóstico', value=PUBLIC_KAFKA_HOST,
		help='Usa o IP público informado. Pode ser alterado para outro host acessível.')
	ports = st.multiselect('Portas para testar', options=list(PUBLIC_KAFKA_PORTS),
		default=list(PUBLIC_KAFKA_PORTS))
	if st.button('Testar conectividade (TCP + metadados Kafka)', type='primary'):
		if not host.strip() or not ports:
			st.error('Informe um host e selecione ao menos uma porta.')
			return
		for port in ports:
			with st.spinner(f'Testando {host.strip()}:{port}...'):
				result = probe_broker(host.strip(), port)
			st.markdown(f'**{result.address}**')
			if result.tcp_ok:
				st.success(f'TCP: {result.tcp_detail}')
			else:
				st.error(f'TCP: {result.tcp_detail}')
			if result.kafka_ok:
				st.success(f'Kafka: {result.kafka_detail}')
			else:
				st.warning(f'Kafka: {result.kafka_detail}')
		st.caption('TCP OK comprova somente que a porta aceitou conexão. Metadados Kafka OK indicam '
			'que o cliente conseguiu dialogar com o broker; não comprovam tópicos, contratos ou serviços.')
	st.markdown('**Como usar a porta 9093 no restante do A3**')
	st.code('export A3_KAFKA_BOOTSTRAP_SERVERS=200.17.66.56:9093', language='bash')
	st.caption('O padrão é 9092. Se a porta 9093 for a correta, reinicie o Streamlit após definir a variável. '
		'Caso uma porta abra no TCP mas falhe em Kafka, confira segurança (TLS/SASL) e advertised.listeners no broker.')
