from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI(
    title="Simulador IoT - Proxy de Comunicaciones",
    description="Microservicio asíncrono para gestión de WebSockets y emulación IoT.",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "ok", "service": "FastAPI Communication Proxy"}

@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Asynchronous echo to ensure event loop remains unblocked
            await manager.send_personal_message(f"ACK [{client_id}]: {data}", websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket)