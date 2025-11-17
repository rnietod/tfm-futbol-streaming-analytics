# tfm-futbol-streaming-analytics
Plataforma de Big Data Híbrida para el análisis táctico de partidos de fútbol en tiempo real (simulado).🚀 Este proyecto implementa una arquitectura de Streaming y Batch para procesar más de $100 \text{ GB}$ de data histórica (PostgreSQL) junto con data de partido en vivo simulada.
⚙️ Stack Tecnológico Clave:
    Ingesta: Google Cloud Pub/Sub (simulador Python)Procesamiento 
    Robusto: PySpark Structured Streaming (Análisis de Pitch Control, Formaciones Tácticas K-Means y Métricas de Rendimiento por Jugador).
    Almacenamiento: GCP Cloud SQL (PostgreSQL) para Data Histórica y resultados procesados.
    Visualización: Streamlit (Dashboard interactivo con animación $\le 1 \text{ seg}$).
Características Únicas: El sistema desacopla la animación en vivo del cálculo intensivo de Pitch Control, disparando el análisis táctico profundo de forma asíncrona tras eventos clave (e.g., goles o tiros) para ofrecer insights contextuales al cuerpo técnico.

Por qué funciona:
  Palabras Clave: Incluye términos clave para másteres en Big Data (Streaming, Batch, PySpark, PostgreSQL, Cloud SQL).
  Valor: Resalta la característica principal (Pitch Control) y la ventaja ($\le 1 \text{ seg}$ de animación).
  Híbrido: Deja claro que gestiona tanto datos masivos históricos como el flujo de datos en vivo.
