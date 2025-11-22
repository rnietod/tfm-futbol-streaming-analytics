import json
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

# --- CONFIGURACIÓN ---
PROJECT_ID = "tfm-master-futbol"
BUCKET_NAME = "tfm-datalake-raw-futbol"
DATASET_ID = "staging_football"
TABLE_ID = "raw_events_native"

# --- CONFIGURACIÓN DE PRODUCCIÓN ---
BATCH_SIZE = 1000  # Lotes grandes para velocidad en Cloud


def run_etl():
    print(f"🚀 Iniciando ETL Masivo: gs://{BUCKET_NAME} -> {DATASET_ID}.{TABLE_ID}")

    # 1. Clientes
    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bq_client = bigquery.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
    except Exception as e:
        print(f"❌ Error conectando a GCP: {e}")
        return

    # 2. Tabla BigQuery
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    schema = [
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("file_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("league", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("season", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("payload", "JSON", mode="REQUIRED")
    ]

    try:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingested_at"
        )
        bq_client.create_table(table, exists_ok=True)
        print("✅ Tabla destino verificada.")
    except Exception as e:
        print(f"❌ Error tabla BQ: {e}")
        return

    # 3. Procesamiento
    print("📦 Escaneando bucket (esto puede tardar si hay miles de archivos)...")
    blobs = bucket.list_blobs(prefix="eventing/")

    rows_buffer = []
    total_processed = 0
    total_errors = 0
    count = 0

    print("⚡ Comenzando carga...")

    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue

        count += 1
        # Log ligero cada 100 archivos para no saturar la terminal
        if count % 100 == 0:
            print(f"   ...escaneando archivo #{count} ({blob.name})")

        try:
            # Metadatos del path: eventing/liga/temporada/id.json
            parts = blob.name.split('/')
            league = parts[1] if len(parts) >= 3 else "unknown"
            season = parts[2] if len(parts) >= 3 else "unknown"

            # Descarga y Limpieza
            json_content = blob.download_as_text()
            json_obj = json.loads(json_content)

            row = {
                "ingested_at": datetime.utcnow().isoformat(),
                "file_name": blob.name,
                "league": league,
                "season": season,
                "payload": json.dumps(json_obj)
            }

            rows_buffer.append(row)

            # Insertar Lote
            if len(rows_buffer) >= BATCH_SIZE:
                errors = bq_client.insert_rows_json(table_ref, rows_buffer)
                if errors:
                    print(f"      ⚠️ Error BQ: {errors}")
                    total_errors += len(rows_buffer)
                else:
                    total_processed += len(rows_buffer)
                    print(f"   ✅ Lote guardado. Total: {total_processed}")

                rows_buffer = []  # Limpiar

        except Exception as e:
            print(f"      ❌ Error con archivo {blob.name}: {e}")
            total_errors += 1
            continue

    # Insertar remanente
    if rows_buffer:
        errors = bq_client.insert_rows_json(table_ref, rows_buffer)
        if not errors:
            total_processed += len(rows_buffer)
            print("   ✅ Lote final guardado.")

    print("\n" + "=" * 30)
    print("🏁 ETL FINALIZADO")
    print(f"📊 Total Insertados: {total_processed}")
    print(f"🐛 Total Errores: {total_errors}")
    print("=" * 30)


if __name__ == "__main__":
    run_etl()
