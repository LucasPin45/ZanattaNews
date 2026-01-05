"""
telegram_news_bot.py

Monitor Parlamentar Zanatta
- Coleta notícias políticas/institucionais via RSS
- Prioriza recência (notícia > fonte)
- Envia para Telegram com:
    • palavra-chave gatilho
    • contexto (trecho do resumo)
    • LINK COMPLETO (ideal para repasse no WhatsApp)
- Não repete matérias
- Histórico em SQLite + XLSX
- GitHub Actions (cron 15 min)
"""

import os
import time
import sqlite3
import html
from datetime import datetime, timezone, timedelta

import requests
import feedparser
from openpyxl import Workbook, load_workbook


# ============================================================
# TELEGRAM (via Secrets / variáveis de ambiente)
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID nas variáveis de ambiente.")


# ============================================================
# FONTES RSS (política, economia, institucional)
# ============================================================
RSS_FEEDS = [
    # Institucional
    "https://www12.senado.leg.br/noticias/feed",
    "https://www.camara.leg.br/rss/ultimas-noticias.xml",

    # G1
    "https://g1.globo.com/rss/g1/politica/",
    "https://g1.globo.com/rss/g1/economia/",
    "https://g1.globo.com/rss/g1/brasil/",

    # Folha
    "https://feeds.folha.uol.com.br/poder/rss091.xml",
    "https://feeds.folha.uol.com.br/mercado/rss091.xml",

    # Estadão
    "https://politica.estadao.com.br/rss",
    "https://economia.estadao.com.br/rss",

    # CNN Brasil
    "https://www.cnnbrasil.com.br/politica/feed/",

    # Metrópoles
    "https://www.metropoles.com/feed",

    # BBC
    "https://feeds.bbci.co.uk/portuguese/rss.xml",

    # Veja
    "https://veja.abril.com.br/rss/",
]


# ============================================================
# PALAVRAS-CHAVE (GATILHOS)
# ============================================================
MUST_HAVE_ANY = [
    # Congresso / Justiça
    "câmara", "senado", "congresso", "plenário", "comissão",
    "stf", "tcu", "cgu", "pgr", "mpf",

    # Economia / tributos
    "imposto", "imposto de renda", "irpf", "tribut",
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


# ============================================================
# PARÂMETROS
# ============================================================
LOOKBACK_HOURS = 8
MAX_ITEMS_PER_RUN = 20
SLEEP_BETWEEN_SENDS = 0.4

BRT = timezone(timedelta(hours=-3))
CONTEXT_CHARS = 90
MAX_CONTEXT_LEN = 240

DB_PATH = "sent_items.db"
HIST_XLSX = "historico_news.xlsx"


# ============================================================
# BANCO (deduplicação)
# ============================================================
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
            item["id"],
            item["title"],
            item["link"],
            item["source"],
            item["published_at"],
            datetime.now(timezone.utc).isoformat(),
        )
    )
    conn.commit()
    conn.close()


# ============================================================
# HISTÓRICO XLSX
# ============================================================
def init_xlsx():
    if os.path.exists(HIST_XLSX):
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "Historico"
    ws.append([
        "Enviado_BRT", "Publicado_BRT", "Fonte",
        "Titulo", "Link", "Keyword", "Contexto"
    ])
    wb.save(HIST_XLSX)


def append_xlsx(row):
    wb = load_workbook(HIST_XLSX)
    ws = wb.active
    ws.append(row)
    wb.save(HIST_XLSX)


# ============================================================
# TELEGRAM
# ============================================================
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=25,
    )
    r.raise_for_status()


# ============================================================
# AUXILIARES
# ============================================================
def normalize(s):
    return (s or "").strip()


def parse_published_dt(entry):
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    return None


def find_context(summary):
    raw = normalize(summary)
    low = raw.lower()

    for kw in MUST_HAVE_ANY:
        k = kw.lower()
        idx = low.find(k)
        if idx != -1:
            start = max(0, idx - CONTEXT_CHARS)
            end = min(len(raw), idx + len(k) + CONTEXT_CHARS)
            ctx = raw[start:end]
            ctx = ctx.replace(kw, f"<b>{kw}</b>")
            if len(ctx) > MAX_CONTEXT_LEN:
                ctx = ctx[:MAX_CONTEXT_LEN] + "…"
            return kw, html.escape(ctx, quote=False)
    return "", ""


# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================
def run():
    init_db()
    init_xlsx()

    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    items = []

    for feed in RSS_FEEDS:
        parsed = feedparser.parse(feed)
        source = parsed.feed.title if hasattr(parsed.feed, "title") else feed

        for e in parsed.entries[:120]:
            title = normalize(getattr(e, "title", ""))
            link = normalize(getattr(e, "link", ""))
            summary = normalize(getattr(e, "summary", ""))

            if not title or not link:
                continue

            blob = f"{title}\n{summary}".lower()
            if not any(k in blob for k in MUST_HAVE_ANY):
                continue
            if any(b in blob for b in BLOCKLIST):
                continue

            pub_dt = parse_published_dt(e)
            if pub_dt and pub_dt < cutoff:
                continue

            kw, ctx = find_context(summary)

            items.append({
                "id": getattr(e, "id", link),
                "title": title,
                "link": link,
                "source": source,
                "published_at": pub_dt.isoformat() if pub_dt else None,
                "published_brt": pub_dt.astimezone(BRT).strftime("%d/%m %H:%M") if pub_dt else "",
                "kw": kw,
                "ctx": ctx,
            })

    # Ordena por mais recente
    items.sort(key=lambda x: x["published_at"] or "", reverse=True)

    sent = 0
    for it in items:
        if sent >= MAX_ITEMS_PER_RUN:
            break
        if was_sent(it["id"]):
            continue

        msg = (
            f"📰 <b>{html.escape(it['title'])}</b>\n"
            f"🏷 <i>{html.escape(it['source'])} • {it['published_brt']} (BRT)</i>\n"
            f"🔎 <b>Gatilho:</b> <code>{html.escape(it['kw'])}</code>\n"
            f"🧾 <i>{it['ctx']}</i>\n"
            f"🔗 {it['link']}"
        )

        send_telegram(msg)

        append_xlsx([
            datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S"),
            it["published_brt"],
            it["source"],
            it["title"],
            it["link"],
            it["kw"],
            it["ctx"],
        ])

        mark_sent(it)
        sent += 1
        time.sleep(SLEEP_BETWEEN_SENDS)

    print(f"OK: enviadas {sent} notícias.")


if __name__ == "__main__":
    run()
