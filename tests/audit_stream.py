import asyncio
import websockets
import json
import sys

# URL de tu API (sin tocar nada)
URI = "ws://localhost:8000/ws/match/test_match"

async def audit_stream():
    print(f"🕵️ AUDITORÍA INICIADA: Conectando a {URI}...")
    try:
        async with websockets.connect(URI) as websocket:
            print("✅ Conexión establecida. Escuchando eventos...\n")
            
            seen_indices = set()
            duplicates = 0
            
            while True:
                msg = await websocket.recv()
                data = json.loads(msg)
                
                if data['type'] == 'event':
                    payload = data['payload']
                    idx = payload.get('index')
                    evt = payload.get('event_type_name')
                    
                    # Chequeo de integridad
                    if idx in seen_indices:
                        print(f"❌ FALLO DE INTEGRIDAD: Índice duplicado detectado: {idx} ({evt})")
                        duplicates += 1
                    else:
                        print(f"✅ Evento Único Recibido: Index {idx} | {evt}")
                        seen_indices.add(idx)
                        
    except KeyboardInterrupt:
        print("\n🛑 Auditoría finalizada por usuario.")
    except Exception as e:
        print(f"⚠️ Error de conexión: {e}")

if __name__ == "__main__":
    # Requiere: pip install websockets
    try:
        import websockets
        asyncio.run(audit_stream())
    except ImportError:
        print("Error: Necesitas instalar 'websockets' para auditar (pip install websockets)")