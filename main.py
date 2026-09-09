from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from mqtt_broker import mqtt_broker

app = FastAPI(
    title="Simulador IoT - Proxy & Broker MQTT",
    description="Microservicio de simulación de comunicaciones IoT en tiempo real.",
    version="0.2.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

class PublishRequest(BaseModel):
    topic: str
    payload: str

@app.get("/health", tags=["Health"])
async def health_check():
    return {"status": "ok", "service": "FastAPI MQTT & WebSocket Proxy"}

@app.get("/mqtt/status", tags=["MQTT Monitoreo"])
async def get_mqtt_status():
    """Retorna las estadísticas actuales del Broker MQTT en memoria."""
    return mqtt_broker.get_stats()

@app.post("/mqtt/publish", tags=["MQTT Monitoreo"])
async def publish_mqtt_message(request: PublishRequest):
    """Permite publicar un paquete MQTT manualmente desde Swagger UI hacia la interfaz gráfica."""
    receivers_count = await mqtt_broker.publish(request.topic, request.payload)
    return {
        "status": "published",
        "topic": request.topic,
        "payload": request.payload,
        "receivers": receivers_count
    }

@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            topic = data.get("topic")

            if not topic:
                await websocket.send_json({"error": "El campo 'topic' es obligatorio."})
                continue

            if action == "subscribe":
                mqtt_broker.subscribe(topic, websocket)
                await websocket.send_json({
                    "type": "system",
                    "status": "subscribed",
                    "topic": topic
                })

            elif action == "unsubscribe":
                mqtt_broker.unsubscribe(websocket, topic)
                await websocket.send_json({
                    "type": "system",
                    "status": "unsubscribed",
                    "topic": topic
                })

            elif action == "publish":
                payload = str(data.get("payload", ""))
                recipients = await mqtt_broker.publish(topic, payload, sender=websocket)
                await websocket.send_json({
                    "type": "system",
                    "status": "published",
                    "topic": topic,
                    "recipients": recipients
                })

            else:
                await websocket.send_json({"error": f"Acción desconocida: {action}"})

    except WebSocketDisconnect:
        mqtt_broker.unsubscribe(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)