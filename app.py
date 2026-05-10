import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import requests
import json
import io
import re
import hashlib

st.set_page_config(
    page_title="Finanz-Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = "finanz.db"

CATEGORIES = [
    "🛒 Lebensmittel & Drogerie",
    "🏠 Miete & Nebenkosten",
    "🚗 Transport & Mobilität",
    "🍽️ Restaurant & Lieferservice",
    "👕 Shopping & Kleidung",
    "🎬 Unterhaltung & Freizeit",
    "💊 Gesundheit & Medizin",
    "🔒 Versicherungen & Finanzen",
    "💰 Gehalt & Einnahmen",
    "💸 Transfers & Sonstiges",
]

# ── Datenbank ─────────────────────────────────────────────────────────────────

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                buchungsdatum    TEXT    NOT NULL,
                empfaenger       TEXT    DEFAULT '',
                verwendungszweck TEXT    DEFAULT '',
                betrag           REAL    NOT NULL,
                waehrung         TEXT    DEFAULT 'EUR',
                kategorie        TEXT,
                bestaetigt       INTEGER DEFAULT 0,
                hash             TEXT    UNIQUE NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)


def get_setting(key, default=None):
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def save_setting(key, value):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, value))


def load_transactions():
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("SELECT * FROM transactions ORDER BY buchungsdatum DESC", conn)
    if not df.empty:
        df["buchungsdatum"] = pd.to_datetime(df["buchungsdatum"])
    return df


def insert_new(df: pd.DataFrame) -> int:
    added = 0
    with sqlite3.connect(DB_PATH) as conn:
        for _, r in df.iterrows():
            cur = conn.execute(
                """INSERT OR IGNORE INTO transactions
                   (buchungsdatum, empfaenger, verwendungszweck, betrag, waehrung, hash)
                   VALUES (?,?,?,?,?,?)""",
                (
                    r["buchungsdatum"].strftime("%Y-%m-%d"),
                    r["empfaenger"],
                    r["verwendungszweck"],
                    r["betrag"],
                    r.get("waehrung", "EUR"),
                    r["hash"],
                ),
            )
            added += cur.rowcount
    return added


def bulk_set_category(pairs: list):
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            "UPDATE transactions SET kategorie=?, bestaetigt=0 WHERE id=?",
            [(cat, tid) for tid, cat in pairs],
        )


def confirm_transaction(tid, kat):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE transactions SET kategorie=?, bestaetigt=1 WHERE id=?", (kat, tid)
        )


def confirm_all():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE transactions SET bestaetigt=1 WHERE bestaetigt=0")


# ── ING CSV Parser ────────────────────────────────────────────────────────────

def parse_ing_csv(raw: bytes) -> pd.DataFrame:
    text = None
    for enc in ("utf-8-sig", "latin-1", "utf-8"):
        try:
            text = raw.decode(enc)
            break
        except Exception:
            continue
    if text is None:
        raise ValueError("Datei konnte nicht gelesen werden.")

    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    start = next((i for i, l in enumerate(lines) if "Buchung" in l and ";" in l), None)
    if start is None:
        raise ValueError(
            "Kein ING-Format erkannt.\n"
            "Exportiere unter: Girokonto → Umsätze → Herunterladen → CSV auswählen."
        )

    # Manuelles Parsing: ING-Felder können Semikolons enthalten
    # Struktur: Datum;Datum;Empfänger;Buchungstext;Verwendungszweck;Saldo;Währung;Betrag;Währung
    # Die letzten 4 Felder sind immer Saldo;Währung;Betrag;Währung
    records = []
    for line in lines[start + 1:]:
        line = line.strip()
        if not line:
            continue
        parts = line.split(";")
        if len(parts) < 9:
            continue
        if not re.match(r"\d{2}\.\d{2}\.\d{4}", parts[0]):
            continue
        records.append({
            "buchungsdatum": parts[0],
            "empfaenger":    parts[2],
            "verwendungszweck": ";".join(parts[4:-4]) if len(parts) > 9 else parts[4],
            "betrag":        parts[-2].replace(".", "").replace(",", "."),
            "waehrung":      parts[-1],
        })

    if not records:
        raise ValueError("Keine Buchungen gefunden. Bitte ING Girokonto CSV verwenden.")

    df = pd.DataFrame(records)

    df["buchungsdatum"] = pd.to_datetime(df["buchungsdatum"], format="%d.%m.%Y")
    df["betrag"] = df["betrag"].astype(float)
    for col in ("empfaenger", "verwendungszweck"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").str.strip()
    if "waehrung" not in df.columns:
        df["waehrung"] = "EUR"

    df["hash"] = df.apply(
        lambda r: hashlib.md5(
            f"{r['buchungsdatum']}|{r['empfaenger']}|{r['verwendungszweck']}|{r['betrag']}".encode()
        ).hexdigest(),
        axis=1,
    )
    return df


# ── OpenRouter KI ─────────────────────────────────────────────────────────────

def ai_categorize(rows: list, api_key: str, model: str) -> list:
    lines = "\n".join(
        f"{i+1}. Empfänger: \"{r['empfaenger']}\", Zweck: \"{r['verwendungszweck']}\", "
        f"Betrag: {r['betrag']:.2f} EUR"
        for i, r in enumerate(rows)
    )
    cats_str = "\n".join(f"- {c}" for c in CATEGORIES)
    prompt = (
        f"Kategorisiere diese deutschen Banktransaktionen.\n\n"
        f"Verfügbare Kategorien:\n{cats_str}\n\n"
        f"Transaktionen:\n{lines}\n\n"
        f"Antworte NUR mit einem JSON-Array (exakte Kategorienamen) in gleicher Reihenfolge.\n"
        f"Positive Beträge → meist '💰 Gehalt & Einnahmen'\n"
        f"Negative Beträge → passende Ausgaben-Kategorie"
    )
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
        },
        timeout=60,
    )
    resp.raise_for_status()
    text = resp.json()["choices"][0]["message"]["content"]
    m = re.search(r"\[.*?\]", text, re.DOTALL)
    if not m:
        raise ValueError(f"KI-Antwort nicht parsebar: {text[:300]}")
    result = json.loads(m.group())
    # ensure valid categories only
    return [(c if c in CATEGORIES else "💸 Transfers & Sonstiges") for c in result]


# ── Seite: Dashboard ──────────────────────────────────────────────────────────

def page_dashboard():
    st.title("📊 Dashboard")
    df = load_transactions()
    if df.empty:
        st.info("Noch keine Daten. Gehe zu **📂 Importieren**.")
        return

    col1, col2 = st.columns([2, 3])
    with col1:
        min_d = df["buchungsdatum"].dt.date.min()
        max_d = df["buchungsdatum"].dt.date.max()
        dr = st.date_input("Zeitraum", (min_d, max_d), min_value=min_d, max_value=max_d)
    with col2:
        sel_cats = st.multiselect("Kategorien", CATEGORIES, placeholder="Alle anzeigen")

    if len(dr) == 2:
        df = df[
            (df["buchungsdatum"].dt.date >= dr[0])
            & (df["buchungsdatum"].dt.date <= dr[1])
        ]
    if sel_cats:
        df = df[df["kategorie"].isin(sel_cats)]
    if df.empty:
        st.warning("Keine Daten für diesen Filter.")
        return

    ein = df[df["betrag"] > 0]["betrag"].sum()
    aus = df[df["betrag"] < 0]["betrag"].sum()
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("💚 Einnahmen", f"{ein:,.2f} €")
    k2.metric("🔴 Ausgaben", f"{abs(aus):,.2f} €")
    k3.metric("💵 Saldo", f"{ein + aus:,.2f} €")
    k4.metric("📋 Buchungen", len(df))

    st.divider()

    df["monat"] = df["buchungsdatum"].dt.to_period("M").astype(str)
    monthly = (
        df.groupby("monat")
        .agg(
            Einnahmen=("betrag", lambda x: x[x > 0].sum()),
            Ausgaben=("betrag", lambda x: x[x < 0].abs().sum()),
        )
        .reset_index()
        .sort_values("monat")
    )

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Einnahmen vs. Ausgaben")
        fig = go.Figure(
            [
                go.Bar(name="Einnahmen", x=monthly["monat"], y=monthly["Einnahmen"], marker_color="#27ae60"),
                go.Bar(name="Ausgaben", x=monthly["monat"], y=monthly["Ausgaben"], marker_color="#e74c3c"),
            ]
        )
        fig.update_layout(
            barmode="group",
            xaxis_tickangle=-30,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(t=10, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Ausgaben nach Kategorie")
        kat = df[df["betrag"] < 0].copy()
        kat["abs"] = kat["betrag"].abs()
        kat_sum = kat.groupby("kategorie")["abs"].sum().reset_index().dropna()
        if not kat_sum.empty:
            fig2 = px.pie(
                kat_sum,
                names="kategorie",
                values="abs",
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", margin=dict(t=10, b=10))
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Noch keine kategorisierten Ausgaben.")

    st.subheader("Kumulierter Saldo-Verlauf")
    ds = df.sort_values("buchungsdatum").copy()
    ds["saldo"] = ds["betrag"].cumsum()
    fig3 = px.line(
        ds,
        x="buchungsdatum",
        y="saldo",
        labels={"buchungsdatum": "Datum", "saldo": "Saldo (€)"},
    )
    fig3.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=10, b=10),
    )
    st.plotly_chart(fig3, use_container_width=True)

    with st.expander("Alle Buchungen anzeigen"):
        show = df[["buchungsdatum", "empfaenger", "verwendungszweck", "betrag", "kategorie"]].copy()
        show["buchungsdatum"] = show["buchungsdatum"].dt.strftime("%d.%m.%Y")
        show.columns = ["Datum", "Empfänger", "Verwendungszweck", "Betrag (€)", "Kategorie"]
        st.dataframe(show, use_container_width=True, hide_index=True)


# ── Seite: Importieren ────────────────────────────────────────────────────────

def page_import():
    st.title("📂 Importieren")

    with st.expander("Wie exportiere ich meine ING-CSV?"):
        st.markdown(
            """
**Schritt-für-Schritt:**
1. [ing.de](https://www.ing.de) öffnen und einloggen
2. **Girokonto** auswählen
3. Auf **Umsätze** klicken
4. Oben rechts: **Herunterladen** → Format **CSV** wählen
5. Die Datei unten hochladen ↓
            """
        )

    f = st.file_uploader("ING CSV hochladen", type="csv")
    if f is None:
        return

    try:
        df = parse_ing_csv(f.read())
    except ValueError as e:
        st.error(f"❌ {e}")
        return

    von = df["buchungsdatum"].min().strftime("%d.%m.%Y")
    bis = df["buchungsdatum"].max().strftime("%d.%m.%Y")
    st.success(f"✅ **{len(df)} Transaktionen** erkannt ({von} – {bis})")
    st.dataframe(
        df[["buchungsdatum", "empfaenger", "verwendungszweck", "betrag"]].head(5),
        use_container_width=True,
        hide_index=True,
    )

    api_key = get_setting("openrouter_api_key")
    model = get_setting("openrouter_model", "meta-llama/llama-3.1-8b-instruct:free")
    btn_label = "📥 Importieren & KI kategorisieren" if api_key else "📥 Importieren (ohne KI)"

    if st.button(btn_label, type="primary"):
        added = insert_new(df)
        if added == 0:
            st.info("Alle Transaktionen waren bereits vorhanden — nichts Neues importiert.")
            return

        st.success(f"✅ {added} neue Buchungen importiert.")

        if api_key:
            with sqlite3.connect(DB_PATH) as conn:
                uncats = pd.read_sql(
                    "SELECT id, empfaenger, verwendungszweck, betrag FROM transactions WHERE kategorie IS NULL",
                    conn,
                ).to_dict("records")

            if uncats:
                bar = st.progress(0, text="KI kategorisiert …")
                pairs = []
                bs = 15
                for i in range(0, len(uncats), bs):
                    batch = uncats[i : i + bs]
                    try:
                        cats = ai_categorize(batch, api_key, model)
                        cats = (cats + ["💸 Transfers & Sonstiges"] * len(batch))[: len(batch)]
                        pairs += list(zip([r["id"] for r in batch], cats))
                    except Exception as e:
                        st.warning(f"KI-Fehler: {e}")
                        pairs += [(r["id"], "💸 Transfers & Sonstiges") for r in batch]
                    bar.progress(
                        min((i + bs) / len(uncats), 1.0),
                        text=f"KI kategorisiert … {min(i+bs, len(uncats))}/{len(uncats)}",
                    )
                bulk_set_category(pairs)
                bar.empty()
                st.success(f"🤖 {len(pairs)} Buchungen kategorisiert. Gehe zu **✅ Bestätigen** zum Prüfen.")
        else:
            st.info("💡 Trage deinen OpenRouter API-Key unter **⚙️ Einstellungen** ein für automatische KI-Kategorisierung.")


# ── Seite: Bestätigen ─────────────────────────────────────────────────────────

def page_confirm():
    st.title("✅ Kategorien bestätigen")

    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql(
            "SELECT * FROM transactions WHERE bestaetigt=0 ORDER BY buchungsdatum DESC",
            conn,
        )

    if df.empty:
        st.success("🎉 Alles bestätigt!")
        return

    st.info(f"**{len(df)}** Buchungen warten auf Bestätigung.")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("✅ Alle auf einmal bestätigen", use_container_width=True):
            confirm_all()
            st.rerun()
    with col2:
        st.caption("Oder einzeln unten korrigieren & bestätigen")

    st.divider()
    df["buchungsdatum"] = pd.to_datetime(df["buchungsdatum"])

    for _, row in df.iterrows():
        with st.container(border=True):
            c1, c2, c3, c4 = st.columns([1.5, 2.5, 2.5, 0.6])
            sign = "🟢 +" if row["betrag"] > 0 else "🔴 "
            c1.write(f"**{row['buchungsdatum'].strftime('%d.%m.%Y')}**")
            c1.write(f"{sign}{row['betrag']:.2f} €")
            c2.write(f"**{row['empfaenger'] or '—'}**")
            zweck = str(row["verwendungszweck"])
            c2.caption(zweck[:55] + "…" if len(zweck) > 55 else zweck)
            cur = row["kategorie"] if row["kategorie"] in CATEGORIES else CATEGORIES[-1]
            new_cat = c3.selectbox(
                "",
                CATEGORIES,
                index=CATEGORIES.index(cur),
                key=f"sel_{row['id']}",
                label_visibility="collapsed",
            )
            if c4.button("✓", key=f"ok_{row['id']}", type="primary"):
                confirm_transaction(row["id"], new_cat)
                st.rerun()


# ── Seite: Einstellungen ──────────────────────────────────────────────────────

def page_settings():
    st.title("⚙️ Einstellungen")

    st.subheader("OpenRouter API-Key")
    key = st.text_input(
        "API-Key",
        value=get_setting("openrouter_api_key", ""),
        type="password",
        help="Deinen Key findest du auf openrouter.ai unter 'Keys'",
    )
    models = [
        "meta-llama/llama-3.1-8b-instruct:free",
        "google/gemma-2-9b-it:free",
        "mistralai/mistral-7b-instruct",
        "openai/gpt-4o-mini",
    ]
    cur_m = get_setting("openrouter_model", models[0])
    model = st.selectbox(
        "Modell",
        models,
        index=models.index(cur_m) if cur_m in models else 0,
        help="':free' Modelle sind kostenlos aber manchmal langsamer",
    )
    if st.button("💾 Speichern", type="primary"):
        if key:
            save_setting("openrouter_api_key", key)
        save_setting("openrouter_model", model)
        st.success("✅ Gespeichert!")

    st.divider()
    st.subheader("Datensicherung")
    st.caption("Da die App online läuft, können Daten bei langen Pausen zurückgesetzt werden. Exportiere regelmäßig.")
    df_all = load_transactions()
    if not df_all.empty:
        csv_bytes = df_all.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇️ Alle Daten als CSV exportieren",
            csv_bytes,
            "finanz_backup.csv",
            "text/csv",
        )
    else:
        st.info("Noch keine Daten vorhanden.")

    st.divider()
    with st.expander("⚠️ Alle Daten löschen"):
        if st.button("🗑️ Alle Transaktionen löschen", type="secondary"):
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute("DELETE FROM transactions")
            st.success("Gelöscht.")
            st.rerun()


# ── Main ──────────────────────────────────────────────────────────────────────

init_db()

with st.sidebar:
    st.title("💰 Meine Finanzen")
    page = st.radio(
        "Navigation",
        ["📊 Dashboard", "📂 Importieren", "✅ Bestätigen", "⚙️ Einstellungen"],
        label_visibility="collapsed",
    )
    st.divider()
    df_side = load_transactions()
    if not df_side.empty:
        unconf = int((df_side["bestaetigt"] == 0).sum())
        st.caption(f"📋 {len(df_side)} Buchungen gesamt")
        if unconf:
            st.warning(f"⏳ {unconf} zu bestätigen")
        else:
            st.caption("✅ Alle bestätigt")

{
    "📊 Dashboard": page_dashboard,
    "📂 Importieren": page_import,
    "✅ Bestätigen": page_confirm,
    "⚙️ Einstellungen": page_settings,
}[page]()
