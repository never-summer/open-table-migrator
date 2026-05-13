from textwrap import dedent

from skills.data_lineage.extractors.kafka import extract


def test_kafka_template_send_with_string_topic():
    code = dedent('''
        public class Pub {
            public void emit(ClientUpdateEvent ev) {
                kafkaTemplate.send("client-updates", ev);
            }
        }
    ''').encode()
    sites = extract(code, file="Pub.java")
    sends = [s for s in sites if s.kind == "kafka_send"]
    assert len(sends) == 1
    assert sends[0].topic == "client-updates"
    assert sends[0].payload_var == "ev"


def test_kafka_listener_with_topics_attribute():
    code = dedent('''
        public class L {
            @KafkaListener(topics = "client-updates")
            public void onMsg(ClientUpdateEvent ev) {}
        }
    ''').encode()
    sites = extract(code, file="L.java")
    listens = [s for s in sites if s.kind == "kafka_listen"]
    assert len(listens) == 1
    assert listens[0].topic == "client-updates"
    assert listens[0].payload_type == "ClientUpdateEvent"


def test_send_with_non_string_topic_skipped():
    code = dedent('''
        public class P {
            public void emit(String topic, Foo ev) {
                kafkaTemplate.send(topic, ev);
            }
        }
    ''').encode()
    sites = extract(code, file="P.java")
    assert all(s.kind != "kafka_send" for s in sites)
