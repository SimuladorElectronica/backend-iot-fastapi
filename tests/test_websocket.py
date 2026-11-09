import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_swagger_docs_available():
    response = client.get("/docs")
    assert response.status_code == 200

def test_websocket_handshake_and_json_protocol():
    client_id = "test_esp32_01"

    # Valida el handshake exitoso y la respuesta del protocolo ante payload JSON
    with client.websocket_connect(f"/ws/{client_id}") as websocket:
        # Enviar paquete de prueba sin tópico obligatorio para validar respuesta del servidor
        websocket.send_json({"action": "subscribe"})

        response = websocket.receive_json()
        assert "error" in response
        assert response["error"] == "El campo 'topic' es obligatorio."


def test_websocket_unknown_action_returns_error():
    with client.websocket_connect("/ws/test_unknown_action") as websocket:
        websocket.send_json({"action": "bogus", "topic": "escuela/temperatura"})

        response = websocket.receive_json()
        assert response["error"] == "Acción desconocida: bogus"


def test_websocket_unsubscribe_acknowledgement():
    with client.websocket_connect("/ws/test_unsubscribe") as websocket:
        websocket.send_json({"action": "subscribe", "topic": "escuela/humedad"})
        assert websocket.receive_json()["status"] == "subscribed"

        websocket.send_json({"action": "unsubscribe", "topic": "escuela/humedad"})
        response = websocket.receive_json()
        assert response["status"] == "unsubscribed"
        assert response["topic"] == "escuela/humedad"