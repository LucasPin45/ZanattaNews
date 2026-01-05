"""
telegram_news_bot.py

Prioridade = a notícia (mais recente primeiro), independente da fonte.

NOVO (REGRA ATUAL):
- Contexto vem SEMPRE do RESUMO (summary)
- Palavra-chave que disparou aparece:
    • Identificada
    • Destacada em negrito no contexto
- Palavras-chave ampliadas (Governo Federal)

Mantém:
- RSS múltiplas fontes
- Filtro MUST_HAVE_ANY + BLOCKLIST
- Ordenação por recência
- Deduplicação (SQLite)
- Histórico em XLSX
- GitHub Actions (15 min)
"""

import os
import time
import sqlite3
import argparse
import html
from datetime import datetime, timezone, timedelta

import requests
import feedparser
from openpyxl import Workbook, load_workbook


# =========================
# Telegram
# =========================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# =========================
# FONTES RSS
# =========================
RSS_FEEDS = [
    "https://www12.senado.leg.br/noticias/feed",
    "https://www.camara.leg.br/rss/ultimas-noticias.xml",

    "https://g1.globo.com/rss/g1/politica/",
    "https://g1.globo.com/rss/g1/economia/",
    "https://g1.globo.com/rss/g1/brasil/",

    "https://feeds.folha.uol.com.br/poder/rss091.xml",
    "https://feeds.folha.uol.com.br/mercado/rss091.xml",

    "https://politica.estadao.com.br/rss",
    "https://economia.estadao.com.br/rss",

    "https://www.cnnbrasil.com.br/politica/feed/",
    "https://www.metropoles.com/feed",

    "https://feeds.bbci.co.uk/portuguese/rss.xml",
    "https://veja.abril.com.br/rss/",
]

# =========================
# PALAVRAS-CHAVE (GATILHO)
# =========================
MUST_HAVE_ANY = [
    # Congresso / Justiça
    "câmara", "senado", "congresso", "plenário", "comissão",
    "stf", "tcu", "cgu", "pgr", "mpf",

    # Economia / Tributos
    "imposto", "irpf", "imposto de renda", "tribut",
    "orçamento", "ldo", "ploa", "gastos", "lrf",
    "inflação", "juros", "selic", "banco central", "pix",

    # Governo Federal (NOVO)
    "lula", "haddad", "governo federal", "governo lula",
    "ministério da fazenda", "receita federal",
    "ministério da justiça", "Macaé Evaristo",

    # SC / Mandato
    "santa catarina", "julia zanatta", "zanatta", "Bolsonaro", "Jorginho Mello"
]

BLOCKLIST = [
    "horóscopo", "bbb", "fofoca", "celebridade",
]

BOOST = [
    "câmara", "senado", "stf", "tcu",
    "orçamento", "imposto", "irpf",
    "lula", "haddad", "governo federal",
    "zanatta", "santa catarina",
]

# =========================
# PARÂMETROS
# =========================
LOOKBACK_HOURS = 8
MAX_ITEMS_PER_RUN = 20
SLEEP_BETWEEN_SENDS = 0.35
BRT = timezone(timedelta(hours=-3))

CONTEXT_CHARS = 80
MAX_CONTEXT_LEN = 220

DB_PATH = "sent_items.db"
HIST_XLSX = "historico_news.xlsx"


# =========================
# UTILITÁRIOS
# =========================
def normalize(s: str) -> str:
    return (s or "").strip()


def blocked(text: str) -> bool:
    return any(b in text for b in BLOCKLIST)


def score_item(text: str) -> int:
    return sum(2 for b in BOOST if b in text)


def parse_published_dt(entry):
    for attr in ("published_parsed", "updated_parsed"):
        dt = getattr(entry, attr, None)
        if dt:
            return datetime(*dt[:6], tzinfo=timezone.utc)
    return None


def find_context_from_summary(summary: str):
    s_raw = normalize(summary)
    s = s_raw.lower()

    for kw in MUST_HAVE_ANY:
        k = kw.lower()
        idx = s.find(k)
        if idx != -1:
            start = max(0, idx - CONTEXT_CHARS)
            end = min(len(s_raw), idx + len(kw) + CONTEXT_CHARS)
            ctx = s_raw[start:end]
            return kw, ctx
    return "", ""


def highlight_keyword(ctx: str, kw: str) -> str:
    if not kw:
        return ctx
    return ctx.replace(kw, f"<b>{kw}</b>")


# =========================
# BANCO (SQLite)
# =========================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent (
            id TEXT PRIMARY KEY,
            title TEXT,
            link TEXT,
            source TEXT,
            published_at TEXT,
            sent_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def was_sent(item_id):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sent WHERE id=?", (item_id,))
    r = cur.fetchone()
    conn.close()
    return r is not None


def mark_sent(item):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO sent VALUES (?, ?, ?, ?, ?, ?)",
        (
            item["id"], item["title"], item["link"], item["source"],
            item["published_at"], datetime.now(timezone.utc).isoformat()
        )
    )
    conn.commit()
    conn.close()


# =========================
# XLSX
# =========================
def init_xlsx():
    if os.path.exists(HIST_XLSX):
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "Historico"
    ws.append([
        "Enviado_BRT", "Publicado_BRT", "Fonte",
        "Titulo", "Link", "Score", "Keyword", "Contexto"
    ])
    wb.save(HIST_XLSX)


def append_xlsx(row):
    wb = load_workbook(HIST_XLSX)
    ws = wb.active
    ws.append(row)
    wb.save(HIST_XLSX)


# =========================
# TELEGRAM
# =========================
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }, timeout=25)
    r.raise_for_status()


# =========================
# EXECUÇÃO
# =========================
def run():
    init_db()
    init_xlsx()

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=LOOKBACK_HOURS)

    items = []

    for feed in RSS_FEEDS:
        f = feedparser.parse(feed)
        source = f.feed.title if hasattr(f.feed, "title") else feed

        for e in f.entries[:120]:
            title = normalize(e.title)
            link = normalize(e.link)
            summary = normalize(getattr(e, "summary", ""))

            blob = f"{title}\n{summary}".lower()

            if not any(k in blob for k in MUST_HAVE_ANY):
                continue
            if blocked(blob):
                continue

            pub_dt = parse_published_dt(e)
            if pub_dt and pub_dt < cutoff:
                continue

            kw, ctx = find_context_from_summary(summary)
            ctx = highlight_keyword(html.escape(ctx), kw.lower())

            items.append({
                "id": e.get("id", link),
                "title": title,
                "link": link,
                "source": source,
                "published_at": pub_dt.isoformat() if pub_dt else None,
                "published_brt": pub_dt.astimezone(BRT).strftime("%d/%m %H:%M") if pub_dt else "",
                "score": score_item(blob),
                "kw": kw,
                "ctx": ctx
            })

    items.sort(key=lambda x: (x["published_at"] or "", x["score"]), reverse=True)

    sent = 0
    for it in items:
        if sent >= MAX_ITEMS_PER_RUN or was_sent(it["id"]):
            continue

        msg = (
            f"📰 <b>{html.escape(it['title'])}</b>\n"
            f"🏷 <i>{html.escape(it['source'])} • {it['published_brt']} (BRT)</i>\n"
            f"🔎 <b>Gatilho:</b> <code>{html.escape(it['kw'])}</code>\n"
            f"🧾 <i>{it['ctx']}</i>\n"
            f"🔗 <a href=\"{it['link']}\">Abrir</a>"
        )

        send_telegram(msg)

        append_xlsx([
            datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S"),
            it["published_brt"],
            it["source"],
            it["title"],
            it["link"],
            it["score"],
            it["kw"],
            it["ctx"]
        ])

        mark_sent(it)
        sent += 1
        time.sleep(SLEEP_BETWEEN_SENDS)

    print(f"OK: enviadas {sent} notícias.")


if __name__ == "__main__":
    run()
