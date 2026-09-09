import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_swagger_docs_available():
    response = client.get("/docs")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_websocket_handshake_and_echo():
    client_id = "test_esp32_01"
    
    # Handshake y simulación de ciclo de vida del WebSocket
    with client.websocket_connect(f"/ws/{client_id}") as websocket:
        payload = "ping_payload"
        websocket.send_text(payload)
        
        data = websocket.receive_text()
        assert data == f"ACK [{client_id}]: {payload}"