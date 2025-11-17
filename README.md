# FUTBOL-STREAMING-ANALYTICS: Sistema Híbrido de Análisis en Vivo y Táctico para Cuerpos Técnicos ⚽

Plataforma de **Big Data Híbrida** diseñada para el análisis táctico de partidos de fútbol en tiempo real (simulado).

Este proyecto (TFM) se centra en la gestión de data histórica masiva (más de **100 GB** de Eventing) para análisis Pre-Partido, combinada con el procesamiento de un flujo de datos en vivo de baja latencia para la toma de decisiones tácticas en el momento.

---

## ⚙️ Arquitectura y Stack Tecnológico Clave

La solución está dividida en dos flujos principales, ambos gestionados por un entorno distribuido:

### 1. Flujo Batch (Análisis Pre-Partido)
* **Función:** Carga, procesamiento y modelado de $100 \text{ GB}$ de data histórica para generar informes estáticos de rendimiento y modelos predictivos (e.g., rendimiento comparativo de jugadores).
* **Herramientas:** **GCP Cloud SQL** (PostgreSQL) y **PySpark (Modo Batch)**.

### 2. Flujo Streaming (Análisis en Vivo)
* **Función:** Simulación de un partido en vivo con animaciones fluidas ($\le 1 \text{ seg}$) y análisis táctico profundo.
* **Herramientas:** **GCP Pub/Sub**, **PySpark Structured Streaming**.

### 🔑 Core Features & Metodología

| Característica | Detalle | Justificación PySpark |
| :--- | :--- | :--- |
| **Pitch Control Asíncrono** | Cálculo intensivo del control de área del campo, disparado bajo demanda o tras **eventos clave** (Goles, Tiros) para análisis contextual. | Uso de **PySpark UDFs** para cálculos geoespaciales complejos sobre DataFrames. |
| **Líneas de Formación** | Identificación automática de la formación táctica (e.g., 4-4-2) del equipo basada en la posición promedio de los jugadores. | Uso de **PySpark MLlib (K-Means Clustering)**. |
| **Animación Fluida** | Visualización en vivo del movimiento de los 22 jugadores. | PySpark actúa como un *passthrough* rápido, y **Streamlit** se encarga del renderizado de baja latencia ($\le 1 \text{ seg}$). |
| **Modelos Comparativos** | Evaluación del rendimiento actual de un jugador frente a su promedio histórico (cálculo Pre-Partido). | Uso de **PySpark SQL** para consultas complejas sobre los $100 \text{ GB}$ de data histórica. |

---

## 🛠️ Entorno de Desarrollo

El proyecto utiliza un entorno aislado (`venvfutbol`) y automatizado en **VS Code**, autenticado directamente con GCP para la gestión de recursos de la nube.
