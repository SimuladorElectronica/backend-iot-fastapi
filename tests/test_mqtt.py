import time
import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_swagger_mqtt_status_endpoint():
    response = client.get("/mqtt/status")
    assert response.status_code == 200
    data = response.json()
    assert "total_topics" in data
    assert "total_messages_published" in data


def test_mqtt_pub_sub_latency_under_50ms():
    topic = "escuela/temperatura"
    payload_val = "25.4"

    # Simulación concurrente: ws_sub (Widget Grafico UI) y ws_pub (ESP32)
    with client.websocket_connect("/ws/ui_dashboard") as ws_sub:
        with client.websocket_connect("/ws/esp32_dev") as ws_pub:
            # 1. Suscribir el Dashboard de la UI
            ws_sub.send_json({"action": "subscribe", "topic": topic})
            sub_ack = ws_sub.receive_json()
            assert sub_ack["status"] == "subscribed"

            # 2. Publicar desde la ESP32 y medir latencia
            start_time = time.perf_counter()

            ws_pub.send_json({
                "action": "publish",
                "topic": topic,
                "payload": payload_val
            })

            # Recepción del paquete retransmitido por el Broker en la UI
            received_msg = ws_sub.receive_json()
            latency_ms = (time.perf_counter() - start_time) * 1000

            # Validaciones de Criterios de Aceptación
            assert received_msg["type"] == "mqtt_message"
            assert received_msg["topic"] == topic
            assert received_msg["payload"] == payload_val
            assert latency_ms < 50.0  # Latencia menor a 50 ms


def test_mqtt_multiple_subscribers():
    topic = "escuela/humedad"
    payload_val = "60%"

    with client.websocket_connect("/ws/widget_1") as sub1:
        with client.websocket_connect("/ws/widget_2") as sub2:
            with client.websocket_connect("/ws/esp32_sensor") as pub:
                sub1.send_json({"action": "subscribe", "topic": topic})
                sub2.send_json({"action": "subscribe", "topic": topic})

                _ = sub1.receive_json()
                _ = sub2.receive_json()

                pub.send_json({"action": "publish", "topic": topic, "payload": payload_val})

                msg1 = sub1.receive_json()
                msg2 = sub2.receive_json()

                assert msg1["payload"] == payload_val
                assert msg2["payload"] == payload_val


def test_swagger_rest_publish_trigger():
    topic = "escuela/luces"
    payload = "ON"

    with client.websocket_connect("/ws/ui_led") as ws_sub:
        ws_sub.send_json({"action": "subscribe", "topic": topic})
        _ = ws_sub.receive_json()

        # Publicar directamente desde la API REST (simulando prueba desde Swagger UI)
        rest_response = client.post("/mqtt/publish", json={"topic": topic, "payload": payload})
        assert rest_response.status_code == 200
        assert rest_response.json()["receivers"] == 1

        # Verificar que la UI recibió el mensaje
        ws_msg = ws_sub.receive_json()
        assert ws_msg["payload"] == payload