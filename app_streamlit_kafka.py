import json, sqlite3, time, random, string, re
import pandas as pd
import streamlit as st
import altair as alt
from collections import Counter

# Kafka
try:
    from confluent_kafka import Producer, Consumer, KafkaError, admin, TopicPartition
    HAS_KAFKA = True
except Exception:
    HAS_KAFKA = False

st.set_page_config(page_title="DB ↔ Kafka (Simple + Monitor + WordCount)", page_icon="🧩", layout="wide")
st.title("🧩 DB ↔ Kafka — Monitoreo + WordCount")

with st.sidebar:
    st.header("Conexión")
    db_path = st.text_input("Ruta BD SQLite", "sample.db")
    table = st.text_input("Tabla", "customers")
    bootstrap = st.text_input("Bootstrap (Kafka)", "localhost:9092")
    topic = st.text_input("Topic destino / consumo", "customers_json")
    simulate = st.checkbox("Simular sin Kafka", value=not HAS_KAFKA)
    st.caption(f"Cliente Kafka disponible: {'Sí' if HAS_KAFKA else 'No'}")

tab_db, tab_mon, tab_wc = st.tabs(["📤 DB → Kafka", "📡 Monitoreo y Consumo", "🧮 WordCount (Streaming)"])

# -------------------- Helpers --------------------
def ensure_table(conn, table):
    conn.execute(f"create table if not exists {table}(id integer primary key, name text, city text, amount real)")

def count_rows(conn, table):
    try:
        cur = conn.execute(f"select count(*) from {table}")
        return cur.fetchone()[0]
    except Exception:
        return 0

def is_internal(name: str) -> bool:
    return name.startswith("__") or name.startswith("_connect")

def topic_message_count(bootstrap, topic, timeout=5):
    """Devuelve (total_msgs, detalle_por_particion) usando low/high watermark."""
    conf = {
        "bootstrap.servers": bootstrap,
        "group.id": "probe-" + "".join(random.choice(string.ascii_lowercase) for _ in range(6)),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    c = Consumer(conf)
    try:
        md = c.list_topics(topic, timeout=timeout)
        tmeta = md.topics.get(topic)
        if tmeta is None or tmeta.error is not None:
            return 0, []
        total = 0
        detail = []
        for pid, _pmeta in tmeta.partitions.items():
            tp = TopicPartition(topic, pid)
            lo, hi = c.get_watermark_offsets(tp, timeout=timeout)
            cnt = max(0, hi - lo)
            total += cnt
            detail.append({"partition": pid, "low": lo, "high": hi, "count": cnt})
        return total, detail
    finally:
        try: c.close()
        except Exception: pass

# Helpers WordCount
WORD_RE = re.compile(r"[A-Za-zÀ-ÿ0-9_]+")
def normalize_text(s: str) -> list[str]:
    return [t.lower() for t in WORD_RE.findall(s or "")]
def wordcount_batch(messages: list[str]) -> Counter:
    c = Counter()
    for msg in messages:
        for w in normalize_text(msg):
            c[w] += 1
    return c

# -------------------- TAB 1: DB -> Kafka --------------------
with tab_db:
    st.subheader("0) Inicializar BD (crear/repoblar tabla)")
    c1, c2, c3 = st.columns(3)
    with c1:
        do_create = st.button("Crear BD demo (3 filas)")
    with c2:
        do_truncate = st.button("Vaciar tabla (DROP/CREATE)")
    with c3:
        do_add = st.button("Insertar 1 fila extra")

    if do_create or do_truncate or do_add:
        try:
            con = sqlite3.connect(db_path)
            if do_create or do_truncate:
                con.execute(f"drop table if exists {table}")
                con.execute(f"create table {table}(id integer primary key, name text, city text, amount real)")
                con.executemany(
                    f"insert into {table}(name, city, amount) values(?,?,?)",
                    [("Ana","Bogotá",120.5), ("Luis","Medellín",89.0), ("Sofía","Cali",150.75)],
                )
                con.commit()
                st.success(f"Tabla '{table}' creada y poblada con 3 filas.")
            if do_add and not (do_create or do_truncate):
                ensure_table(con, table)
                con.execute(f"insert into {table}(name, city, amount) values(?,?,?)", ("Nuevo","Barranquilla",200.0))
                con.commit()
                st.success("Fila extra insertada.")
            st.info(f"Total filas actuales: {count_rows(con, table)}")
        except Exception as e:
            st.error(f"Error al inicializar BD: {e}")
        finally:
            try: con.close()
            except Exception: pass

    col1, col2 = st.columns([1,1])
    with col1:
        st.subheader("1) Datos en la BD")
        try:
            con = sqlite3.connect(db_path)
            df = pd.read_sql_query(f"SELECT * FROM {table}", con)
            st.dataframe(df, use_container_width=True, height=300)
        except Exception as e:
            st.error(f"Error leyendo BD: {e}")
            df = pd.DataFrame()
        finally:
            try: con.close()
            except Exception: pass

    with col2:
        st.subheader("2) Previsualización JSON")
        previews = []
        if not df.empty:
            cols = list(df.columns)
            for _, row in df.iterrows():
                payload = {k: (None if pd.isna(row[k]) else row[k]) for k in cols}
                previews.append(payload)
            st.code(json.dumps(previews, ensure_ascii=False, indent=2))

    st.subheader("3) Publicación")
    cpub1, cpub2 = st.columns([1,1])
    with cpub1:
        btn = st.button("Publicar a Kafka" if not simulate else "Simular publicación", key="publish_btn")
    with cpub2:
        btn_repub = st.button("Re-publicar datos de la BD")

    if (btn or btn_repub):
        if df.empty:
            st.warning("No hay datos para enviar.")
        else:
            if simulate or not HAS_KAFKA:
                st.info("Simulación activada — no se conecta a Kafka.")
                st.success(f"Simulado: se enviarían {len(previews)} mensajes a '{topic}'")
            else:
                progress = st.progress(0, "Enviando...")
                p = Producer({"bootstrap.servers": bootstrap})
                sent = 0
                for i, payload in enumerate(previews, start=1):
                    p.produce(topic, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
                    sent += 1
                    if i % 50 == 0:
                        p.flush(2)
                    progress.progress(i/len(previews), f"Enviando {i}/{len(previews)}")
                p.flush(10)
                progress.progress(1.0, "Completado")
                st.success(f"Enviados {sent} mensajes a '{topic}'")

# -------------------- TAB 2: MONITOREO --------------------
with tab_mon:
    st.subheader("Estado del broker y tópicos")
    colA, colB = st.columns([1,1])

    # ---- A) Metadatos + gráficos
    with colA:
        topics_data = []
        if not HAS_KAFKA:
            st.error("confluent-kafka no está instalado en este entorno.")
        else:
            ac = admin.AdminClient({"bootstrap.servers": bootstrap})
            if st.button("Refrescar metadatos de cluster"):
                try:
                    md = ac.list_topics(timeout=5)
                    st.success(f"Cluster OK — {len(md.topics)} tópicos")
                    for tname, tmeta in md.topics.items():
                        if tname.startswith("__"):
                            continue
                        partitions = len(tmeta.partitions)
                        topics_data.append({"topic": tname, "partitions": partitions})
                    if topics_data:
                        tdf = pd.DataFrame(topics_data)
                        st.dataframe(tdf.sort_values("topic"), use_container_width=True, height=200)
                except Exception as e:
                    st.error(f"No se pudo obtener metadatos: {e}")

    # ---- B) Consumidor rápido
    with colB:
        st.markdown("**Consumidor rápido**")
        num_msgs = st.number_input("Cantidad a leer", min_value=1, max_value=1000, value=10, step=1)
        from_begin = st.checkbox("Desde el comienzo", value=True)
        if st.button("Leer ahora"):
            try:
                gid = "probe-" + "".join(random.choice(string.ascii_lowercase) for _ in range(6))
                conf = {
                    "bootstrap.servers": bootstrap,
                    "group.id": gid,
                    "enable.auto.commit": False,
                    "auto.offset.reset": "earliest" if from_begin else "latest",
                }
                c = Consumer(conf)
                c.subscribe([topic])
                msgs = []
                deadline = time.time() + 5
                while len(msgs) < num_msgs and time.time() < deadline:
                    m = c.poll(0.5)
                    if m is None: continue
                    if m.error(): continue
                    try:
                        payload = json.loads(m.value().decode("utf-8"))
                    except Exception:
                        payload = {"raw": m.value().decode("utf-8","ignore")}
                    msgs.append(payload)
                c.close()
                if msgs:
                    st.dataframe(pd.DataFrame(msgs), use_container_width=True, height=280)
                else:
                    st.info("No se leyeron mensajes.")
            except Exception as e:
                st.error(f"No se pudo consumir: {e}")

# -------------------- TAB 3: WORDCOUNT --------------------
with tab_wc:
    st.subheader("WordCount por lote (consume → cuenta → publica)")

    if not HAS_KAFKA:
        st.error("confluent-kafka no está instalado en este entorno.")
    else:
        col = st.columns(2)
        with col[0]:
            input_topic = st.text_input("Tópico de entrada", value="wordcount_in")
            output_topic = st.text_input("Tópico de salida", value="wordcount_out")
        with col[1]:
            batch_msgs = st.number_input("Máx. mensajes a leer", 1, 10000, 100, step=10)
            timeout_s = st.slider("Tiempo de lectura (seg)", 1, 60, 8)
            top_n = st.slider("Top-N palabras", 5, 50, 20)

        ac = admin.AdminClient({"bootstrap.servers": bootstrap})
        def ensure_topic(topic_name: str, partitions=1, rf=1):
            try:
                md = ac.list_topics(timeout=3)
                if topic_name not in md.topics:
                    fs = ac.create_topics([admin.NewTopic(topic_name, num_partitions=partitions, replication_factor=rf)])
                    fs[topic_name].result()
                    st.success(f"Creado tópico '{topic_name}' ({partitions} particiones)")
            except Exception as e:
                st.warning(f"No se pudo crear/verificar tópico '{topic_name}': {e}")

        if st.button("Asegurar tópicos (in/out)"):
            ensure_topic(input_topic)
            ensure_topic(output_topic)

        with st.expander("✍️ Generar ejemplos (input)"):
            sample_lines = st.text_area("Líneas de ejemplo", value="Hola Kafka desde Streamlit\nKafka es genial para streaming\ndatos datos datos")
            if st.button("Publicar ejemplos"):
                try:
                    p = Producer({"bootstrap.servers": bootstrap})
                    count = 0
                    for line in sample_lines.splitlines():
                        if not line.strip(): continue
                        p.produce(input_topic, line.strip().encode("utf-8"))
                        count += 1
                    p.flush(5)
                    st.success(f"Publicadas {count} líneas en '{input_topic}'")
                except Exception as e:
                    st.error(f"No se pudo publicar: {e}")

        if st.button("Ejecutar WordCount"):
            try:
                gid = "wc-" + "".join(random.choice(string.ascii_lowercase) for _ in range(6))
                conf = {"bootstrap.servers": bootstrap, "group.id": gid, "enable.auto.commit": False, "auto.offset.reset": "earliest"}
                c = Consumer(conf)
                c.subscribe([input_topic])

                raw_lines = []
                deadline = time.time() + timeout_s
                while len(raw_lines) < batch_msgs and time.time() < deadline:
                    m = c.poll(0.5)
                    if m is None: continue
                    if m.error(): continue
                    raw_lines.append(m.value().decode("utf-8","replace"))
                c.close()

                st.info(f"Leídas {len(raw_lines)} líneas")
                counts = wordcount_batch(raw_lines)
                if not counts:
                    st.warning("No tokens encontrados")
                else:
                    df_wc = pd.DataFrame(counts.items(), columns=["word","count"]).sort_values("count",ascending=False)
                    st.dataframe(df_wc.head(top_n), use_container_width=True, height=300)

                    p = Producer({"bootstrap.servers": bootstrap})
                    for w,n in counts.items():
                        payload = json.dumps({"word": w, "count": int(n)}, ensure_ascii=False)
                        p.produce(output_topic, payload.encode("utf-8"))
                    p.flush(10)
                    st.success(f"Publicados {len(counts)} registros en '{output_topic}'")

                    chart = (
                        alt.Chart(df_wc.head(top_n))
                        .mark_bar()
                        .encode(x=alt.X("word:N", sort="-y"), y=alt.Y("count:Q"), tooltip=["word","count"])
                        .properties(height=280)
                    )
                    st.altair_chart(chart, use_container_width=True)
            except Exception as e:
                st.error(f"Falló WordCount: {e}")
