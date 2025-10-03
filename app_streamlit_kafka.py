import json, sqlite3, time, random, string
import pandas as pd
import streamlit as st
import altair as alt

# Kafka
try:
    from confluent_kafka import Producer, Consumer, KafkaError, admin, TopicPartition
    HAS_KAFKA = True
except Exception:
    HAS_KAFKA = False

st.set_page_config(page_title="DB ↔ Kafka (Simple + Monitor)", page_icon="🧩", layout="wide")
st.title("🧩 DB ↔ Kafka — Simple + Monitoreo")

with st.sidebar:
    st.header("Conexión")
    db_path = st.text_input("Ruta BD SQLite", "sample.db")
    table = st.text_input("Tabla", "customers")
    bootstrap = st.text_input("Bootstrap (Kafka)", "localhost:9092")
    topic = st.text_input("Topic destino / consumo", "customers_json")
    simulate = st.checkbox("Simular sin Kafka", value=not HAS_KAFKA)
    st.caption(f"Cliente Kafka disponible: {'Sí' if HAS_KAFKA else 'No'}")

tab_db, tab_mon = st.tabs(["📤 DB → Kafka", "📡 Monitoreo y Consumo"])

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
        btn_repub = st.button("Re-publicar datos de la BD", help="Vuelve a leer la tabla y reenvía todo al tópico")

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

    # ---- A) Metadatos + gráficos + ADMIN de tópicos
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
                            continue  # oculta internos "__*"
                        partitions = len(tmeta.partitions)
                        topics_data.append({"topic": tname, "partitions": partitions, "err": str(tmeta.error) if tmeta.error else ""})
                    if topics_data:
                        tdf = pd.DataFrame(topics_data)
                        tdf["tipo"] = tdf["topic"].apply(lambda s: "interno" if is_internal(s) else "usuario")
                        st.dataframe(tdf.sort_values("topic"), use_container_width=True, height=240)

                        st.markdown("**Particiones por tópico**")
                        chart = (
                            alt.Chart(tdf)
                            .mark_bar()
                            .encode(
                                x=alt.X("topic:N", sort="-y", title="tópico"),
                                y=alt.Y("partitions:Q", title="particiones"),
                                color=alt.Color("tipo:N", legend=alt.Legend(title="tipo")),
                                tooltip=["topic","partitions","tipo"],
                            ).properties(height=240)
                        )
                        st.altair_chart(chart, use_container_width=True)

                        st.markdown("### Volumen de mensajes por tópico (estimado)")
                        calc = st.checkbox("Calcular mensajes por tópico (puede tardar un poco)", value=True, key="calc_counts")
                        if calc:
                            counts = []
                            for tname in tdf["topic"].tolist():
                                total, _detail = topic_message_count(bootstrap, tname, timeout=5)
                                counts.append({"topic": tname, "msg_count": int(total)})
                            cdf = pd.DataFrame(counts).sort_values("msg_count", ascending=False)
                            st.dataframe(cdf, use_container_width=True, height=200)

                            cchart = (
                                alt.Chart(cdf)
                                .mark_bar()
                                .encode(
                                    x=alt.X("topic:N", sort="-y", title="tópico"),
                                    y=alt.Y("msg_count:Q", title="mensajes (estimado)"),
                                    tooltip=["topic","msg_count"],
                                ).properties(height=240)
                            )
                            st.altair_chart(cchart, use_container_width=True)
                    else:
                        st.info("No hay tópicos visibles (o solo internos).")
                except Exception as e:
                    st.error(f"No se pudo obtener metadatos: {e}")

            # --- ADMIN: Crear / Re-crear / Aumentar particiones
            with st.expander("⚙️ Administrar tópico"):
                admin_topic = st.text_input("Nombre de tópico", value=topic, help="Se usará este nombre")
                parts = st.number_input("Particiones deseadas", min_value=1, max_value=200, value=3, step=1)
                repl = st.number_input("Replication factor", min_value=1, max_value=5, value=1, step=1)

                ccol1, ccol2, ccol3 = st.columns([1,1,1])
                if ccol1.button("Crear tópico"):
                    try:
                        fs = ac.create_topics([admin.NewTopic(admin_topic, num_partitions=int(parts), replication_factor=int(repl))])
                        fs[admin_topic].result()
                        st.success(f"Creado '{admin_topic}' con {parts} particiones (RF={repl}).")
                    except Exception as e:
                        st.error(f"No se pudo crear: {e}")

                if ccol2.button("Re-crear (BORRA y crea)"):
                    try:
                        ac.delete_topics([admin_topic])
                        st.warning(f"Solicitada eliminación de '{admin_topic}'…")
                        time.sleep(2)
                        fs = ac.create_topics([admin.NewTopic(admin_topic, num_partitions=int(parts), replication_factor=int(repl))])
                        fs[admin_topic].result()
                        st.success(f"Re-creado '{admin_topic}' con {parts} particiones (RF={repl}).")
                    except Exception as e:
                        st.error(f"No se pudo recrear: {e}")

                if ccol3.button("Aumentar particiones (no reduce)"):
                    try:
                        # Sólo permite INCREMENTAR. No se puede bajar el número de particiones.
                        fs = ac.create_partitions({admin_topic: admin.NewPartitions(int(parts))})
                        fs[admin_topic].result()
                        st.success(f"Aumentadas particiones de '{admin_topic}' a {parts}.")
                    except Exception as e:
                        st.error(f"No se pudo aumentar particiones: {e}")

    # ---- B) Consumidor rápido
    with colB:
        st.markdown("**Consumidor rápido (lectura puntual)**")
        num_msgs = st.number_input("Cantidad a leer", min_value=1, max_value=1000, value=10, step=1)
        from_begin = st.checkbox("Forzar leer TODO desde el comienzo", value=True)
        unique_gid = st.checkbox("Usar group.id único (evita offsets guardados)", value=True)
        timeout_s = st.slider("Tiempo máximo de lectura (seg)", 1, 60, 10)

        if st.button("Leer ahora"):
            try:
                gid_base = "streamlit-probe"
                gid = gid_base + "-" + "".join(random.choice(string.ascii_lowercase) for _ in range(6)) if unique_gid else gid_base
                conf = {
                    "bootstrap.servers": bootstrap,
                    "group.id": gid,
                    "enable.auto.commit": False,
                    "auto.offset.reset": "earliest",
                }
                c = Consumer(conf)

                # asignación manual para fijar offset al low watermark
                md = c.list_topics(topic, timeout=5)
                tmeta = md.topics.get(topic)
                if tmeta is None or tmeta.error is not None:
                    st.error(f"Tópico '{topic}' no encontrado o con error: {tmeta.error if tmeta else 'desconocido'}")
                else:
                    assignments = []
                    for pid, _pmeta in tmeta.partitions.items():
                        tp = TopicPartition(topic, pid)
                        low, high = c.get_watermark_offsets(tp, timeout=5)
                        tp.offset = low if from_begin else -1001  # -1001=Offset.INVALID (usará auto.offset.reset)
                        assignments.append(tp)

                    c.assign(assignments)

                    msgs = []
                    deadline = time.time() + timeout_s
                    while len(msgs) < num_msgs and time.time() < deadline:
                        m = c.poll(0.5)
                        if m is None: 
                            continue
                        if m.error():
                            if m.error().code() == KafkaError._PARTITION_EOF:
                                continue
                            else:
                                st.error(f"Error de consumo: {m.error()}")
                                break

                        raw = m.value()
                        try:
                            payload = json.loads(raw.decode("utf-8"))
                        except Exception:
                            payload = None

                        ts_type, ts_val = m.timestamp()
                        key = None
                        if m.key() is not None:
                            try: key = m.key().decode("utf-8")
                            except Exception: key = str(m.key())
                        headers = {}
                        if m.headers():
                            for k, v in m.headers():
                                headers[k] = (v.decode("utf-8") if isinstance(v,(bytes,bytearray)) else v)

                        row = {
                            "_topic": m.topic(),
                            "_partition": m.partition(),
                            "_offset": m.offset(),
                            "_timestamp": ts_val,
                            "_key": key,
                            "_headers": headers,
                            "json": payload,
                            "raw": raw.decode("utf-8","replace"),
                        }
                        msgs.append(row)

                    c.close()
                    if msgs:
                        st.success(f"Leídos {len(msgs)} mensajes de '{topic}'")
                        st.dataframe(pd.DataFrame(msgs), use_container_width=True, height=320)
                    else:
                        st.info("No se leyeron mensajes (quizá no hay nuevos o el timeout fue corto).")
            except Exception as e:
                st.error(f"No se pudo consumir: {e}")
