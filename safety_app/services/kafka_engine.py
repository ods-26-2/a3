import threading
import json
import uuid
import time
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer

KAFKA_BROKER = "34.66.235.91:9093"
TOPICS = ["ods.s1.zone_transition", "ods.s2.entity", "ods.s2.event"]
TOPIC_ALERT = "ods.s3.alert.command"

class A3KafkaEngine:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(A3KafkaEngine, cls).__new__(cls)
            cls._instance.init_state()
        return cls._instance

    def init_state(self):
        self.violations_ui = []
        self.entity_track_map = {}
        self.alerted_events = {}
        self.recurrence_map = {}
        self.running = False
        self.producer = Producer({'bootstrap.servers': KAFKA_BROKER})

    def start(self):
        if not self.running:
            self.running = True
            threading.Thread(target=self._consume_loop, daemon=True).start()

    def _consume_loop(self):
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'a3-decision-matrix-group',
            'auto.offset.reset': 'latest'
        })
        consumer.subscribe(TOPICS)

        while self.running:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                schema = data.get("schema")
                payload = data.get("payload", {})

                if schema == "ods.s2.entity":
                    self._process_entity(payload)
                elif schema == "ods.s2.event":
                    self._process_event(payload)
            except Exception:
                pass

        consumer.close()

    def _process_entity(self, payload):
        for entity in payload.get("entities", []):
            e_id = entity.get("entity_id")
            track_refs = [t.get("track_id") for t in entity.get("track_refs", [])]
            if e_id:
                self.entity_track_map[e_id] = track_refs

    def _process_event(self, payload):
        if payload.get("status") != "CONFIRMED":
            return

        duration_ms = payload.get("duration_ms", 0)
        details = payload.get("details", {})
        min_duration_ms = details.get("min_duration_ms")

        if min_duration_ms and duration_ms > min_duration_ms:
            entity_id = payload.get("entity_id")
            confidence = payload.get("confidence", 0)

            # Matriz de Decisão (Pesos 60, 20, 20)
            excess_ratio = (duration_ms - min_duration_ms) / min_duration_ms
            score_excess = min(excess_ratio * 100, 100) * 0.6
            score_conf = confidence * 20
            
            rec_count = self.recurrence_map.get(entity_id, 0)
            score_rec = min(rec_count * 5, 20)
            
            total_score = score_excess + score_conf + score_rec

            # Classificação
            if total_score < 40:
                grau, severity = 1, "LOW"
            elif total_score < 75:
                grau, severity = 2, "MEDIUM"
            else:
                grau, severity = 3, "HIGH"

            event_id = payload.get("event_id")
            last_grau = self.alerted_events.get(event_id, 0)

            if grau > last_grau:
                self.alerted_events[event_id] = grau
                self.recurrence_map[entity_id] = rec_count + 1
                
                cam_name = payload.get("source_ids", ["Câmera Desconhecida"])[0]
                
                self.violations_ui.append({
                    "Grau de violação": grau,
                    "Nome da câmera": cam_name,
                    "Horário da violação": datetime.now()
                })
                
                self._publish_s3_alert(payload, severity)

    def _publish_s3_alert(self, event_payload, severity):
        alert_msg = {
            "message_type": "command",
            "message_id": str(uuid.uuid4()),
            "schema": "ods.s3.alert.command",
            "schema_version": "1.0",
            "producer": "A3",
            "published_at": datetime.now(timezone.utc).isoformat(),
            "target": "S3",
            "payload": {
                "title": "Permanência Irregular em Zona",
                "message": f"Entidade {event_payload.get('entity_id')} excedeu limite na zona {event_payload.get('zone_id')}",
                "severity": severity,
                "recipients": ["group:seguranca"],
                "channels": ["SCREEN"]
            }
        }
        self.producer.produce(TOPIC_ALERT, json.dumps(alert_msg).encode('utf-8'))
        self.producer.poll(0)

def get_violations_state():
    engine = A3KafkaEngine()
    return engine.violations_ui