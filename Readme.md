# Módulo 5 — Gestión, Monitoreo y Seguridad (Kafka + Podman)

**Actualizado:** 2025-09-30

Este módulo guía a tus estudiantes a:
- Integrar Kafka con una base de datos usando **Kafka Connect (JDBC Sink)**.
- Construir una app **Kafka Streams** (conteo de palabras).
- Exponer métricas con **kafka-exporter** y consultarlas en **Prometheus**.
- Aplicar **ACLs** para controlar permisos en tópicos y grupos.

## Archivos principales
- https://acatherinebusinessintelligence.github.io/Kafka-gesti-n-monitoreo-seguridad/modulo5_gestion_monitoreo_seguridad.html — Guía paso a paso (recomendada).

> Los archivos auxiliares (por ejemplo `prometheus.yml`, proyecto `streams-wc/`) se crean **durante** el taller siguiendo la guía.

## Prerrequisitos
- Clúster del taller (Zookeeper + 3 brokers) corriendo en la red `kafka-lab_default`.
- Puertos disponibles en el host: 8083, 5432, 9090, 9308, 8080 (opcional).
- Acceso a Internet para instalar el conector JDBC (o usar el Plan B con FileStream).

## Uso rápido
1. Abre la guía en el navegador:
   - Doble clic en `modulo5_gestion_monitoreo_seguridad.html` (o sirve con GitHub Pages).
2. Sigue las secciones:
   - **Kafka Connect** → Postgres + Connect + JDBC Sink.
   - **Kafka Streams** → WordCount.
   - **Prometheus** → kafka-exporter + Prometheus.
   - **ACLs** → activar authorizer y probar permisos.

## Consejos
- Usa tu script `kafka_lab.sh up` para levantar el clúster rápidamente.
- Si la red o DNS no resuelven (`zookeeper`/`kafkaN`), verifica que los contenedores estén conectados a `kafka-lab_default`.
- Para GitHub Pages, coloca el HTML en `docs/` y habilita Pages apuntando a `main /docs`.

---

**Autora:** Alejandra Montaña  
**Licencia:** MIT (o la que prefieras)
