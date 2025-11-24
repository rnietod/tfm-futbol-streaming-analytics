import json
import hashlib
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

# --- CONFIGURACIÓN ---
PROJECT_ID = "tfm-master-futbol"
BUCKET_NAME = "tfm-datalake-raw-futbol"
DATASET_ID = "staging_football"
TABLE_ID = "raw_events_native"

# --- CONFIGURACIÓN DE EJECUCIÓN ---
BATCH_SIZE = 50
TEST_MODE = True
MAX_FILES_TEST = 10


def generate_id(file_name):
    """Crea un ID único (Hash MD5) a partir del nombre del archivo."""
    return hashlib.md5(file_name.encode('utf-8')).hexdigest()


def get_existing_ids(bq_client, table_ref):
    """Descarga los IDs que ya existen en BigQuery para evitar duplicados."""
    print("🔍 Consultando IDs existentes en BigQuery (Deduplicación)...")
    try:
        query = f"SELECT id FROM `{table_ref}`"
        query_job = bq_client.query(query)
        existing = set(row.id for row in query_job)
        print(f"   ✅ Se encontraron {len(existing)} archivos ya cargados.")
        return existing
    except Exception:
        # Si falla (tabla no existe), asumimos vacío.
        # Quitamos 'as e' porque aquí no es crítico imprimir el error exacto
        print("   ℹ️ Tabla no existe o está vacía. Se cargará todo.")
        return set()


def run_etl():
    print("🚀 Iniciando ETL V2.0 (Smart & Safe)")

    # 1. Clientes
    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bq_client = bigquery.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
    except Exception as e:
        print(f"❌ Error conectando a GCP: {e}")
        return

    # 2. Definir Tabla
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
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
        print(f"✅ Tabla destino lista: {TABLE_ID}")
    except Exception as e:
        print(f"❌ Error tabla BQ: {e}")
        return

    # 3. Cargar caché de duplicados
    existing_ids = get_existing_ids(bq_client, table_ref)

    # 4. Escaneo y Proceso
    print("📦 Listando archivos del Bucket...")
    blobs = bucket.list_blobs(prefix="eventing/")

    rows_buffer = []
    count_new = 0
    count_skipped = 0

    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue

        # Generar ID
        file_id = generate_id(blob.name)

        # --- FILTRO DE DUPLICADOS ---
        if file_id in existing_ids:
            count_skipped += 1
            if count_skipped % 500 == 0:
                print(f"   ⏩ Saltados {count_skipped} archivos (Ya existen)...")
            continue

        try:
            # Procesar nuevo archivo
            parts = blob.name.split('/')
            if len(parts) >= 3:
                league = parts[1]
                season = parts[2]
            else:
                league = "unknown"
                season = "unknown"

            json_content = blob.download_as_text()
            json_obj = json.loads(json_content)

            row = {
                "id": file_id,
                "ingested_at": datetime.utcnow().isoformat(),
                "file_name": blob.name,
                "league": league,
                "season": season,
                "payload": json.dumps(json_obj)
            }

            rows_buffer.append(row)

            # --- INSERTAR LOTE (BATCH) ---
            if len(rows_buffer) >= BATCH_SIZE:
                print(f"   📤 Enviando lote de {len(rows_buffer)} registros...")
                errors = bq_client.insert_rows_json(table_ref, rows_buffer)

                if not errors:
                    count_new += len(rows_buffer)
                    print(f"      ✅ Guardado. Total nuevos: {count_new}")
                    for r in rows_buffer:
                        existing_ids.add(r['id'])
                else:
                    print(f"      ⚠️ Error insertando lote: {errors}")

                rows_buffer = []  # Vaciar buffer

        except Exception as e:
            print(f"   ❌ Error leyendo {blob.name}: {e}")
            continue

    # Insertar remanente final
    if rows_buffer:
        print(f"   📤 Enviando lote final ({len(rows_buffer)})...")
        errors = bq_client.insert_rows_json(table_ref, rows_buffer)
        if not errors:
            count_new += len(rows_buffer)
            print("      ✅ Final guardado.")

    print("\n" + "=" * 30)
    print("🏁 ETL TERMINADO")
    print(f"📥 Nuevos Insertados: {count_new}")
    print(f"⏩ Duplicados Saltados: {count_skipped}")
    print("=" * 30)


if __name__ == "__main__":
    run_etl()
