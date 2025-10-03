# 🧩 Kafka + SQLite + Streamlit (DB ↔ Kafka + Monitor)

Aplicación interactiva en **Streamlit** para:
- Cargar datos desde una base SQLite.
- Publicarlos en un tópico de **Apache Kafka** (en formato JSON).
- Monitorear tópicos, particiones y volumen de mensajes.
- Consumir mensajes en tiempo real desde un tópico.
- Administrar tópicos (crear, recrear, aumentar particiones).

---

## 🚀 Requisitos previos

1. **Python 3.9+** instalado en tu máquina.
2. **Kafka corriendo** en `localhost:9092`  
   (puedes levantarlo con Docker/Podman/WSL).

Ejemplo rápido con Docker Compose:

```bash
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
