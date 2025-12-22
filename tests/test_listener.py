import asyncio
import websockets
import json

async def listen():
    uri = "ws://localhost:8000/ws/match/test_match"
    print(f"🔌 Conectando a {uri}...")
    
    async with websockets.connect(uri) as websocket:
        print("✅ Conectado. Esperando eventos (Ctrl+C para salir)...")
        try:
            while True:
                msg = await websocket.recv()
                data = json.loads(msg)
                
                if data['type'] == 'event':
                    evt = data['payload']
                    # Imprimimos SOLO lo vital para ver duplicados
                    print(f"📨 Recibido: Index {evt.get('index')} | {evt.get('event_type_name')}")
        except websockets.exceptions.ConnectionClosed:
            print("❌ Conexión cerrada.")

if __name__ == "__main__":
    asyncio.run(listen())