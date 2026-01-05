"""
telegram_news_bot.py

Regra: prioridade = a notícia (mais recente primeiro), independente da fonte.

O que este bot faz:
- Lê várias fontes RSS (política/economia/institucional)
- Filtra por palavras-chave (MUST_HAVE_ANY) + bloqueio (BLOCKLIST)
- Considera apenas notícias dentro de uma janela de tempo (LOOKBACK_HOURS)
- Ordena por data de publicação (mais novo primeiro) + score de relevância (BOOST)
- Deduplica com SQLite (sent_items.db) e faz migração automática de colunas
- Registra tudo que foi enviado em XLSX (historico_news.xlsx)
- Envia mensagens compactas no Telegram (sem preview grande)

CONFIGURAÇÃO (Windows):
1) Defina variáveis de ambiente:
   setx TELEGRAM_BOT_TOKEN "SEU_TOKEN"
   setx TELEGRAM_CHAT_ID "SEU_CHAT_ID"  (privado: número; grupo: -100...)

2) Feche o terminal e abra outro.

3) Instale dependências:
   py -m pip install requests feedparser openpyxl

4) Rode:
   py telegram_news_bot.py
   ou, para pegar só as últimas 4 horas:
   py telegram_news_bot.py --hours 4
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
# Telegram (via variáveis de ambiente)
# =========================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# =========================
# FONTES RSS (política/economia/institucional)
# Observação: alguns sites mudam RSS ao longo do tempo; se algum quebrar,
# o bot tende a só “pular” aquela fonte.
# =========================
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
    "https://feeds.folha.uol.com.br/mundo/rss091.xml",

    # Estadão
    "https://politica.estadao.com.br/rss",
    "https://economia.estadao.com.br/rss",
    "https://brasil.estadao.com.br/rss",

    # CNN Brasil
    "https://www.cnnbrasil.com.br/politica/feed/",
    "https://www.cnnbrasil.com.br/economia/feed/",

    # Metrópoles (alto volume)
    "https://www.metropoles.com/feed",

    # BBC (contexto internacional)
    "https://feeds.bbci.co.uk/portuguese/rss.xml",

    # Veja (pode ser instável)
    "https://veja.abril.com.br/rss/",
]

# =========================
# FILTROS / RELEVÂNCIA
# Regra: MUST_HAVE_ANY “libera” a notícia se qualquer termo bater em (título+resumo).
# Para reduzir ruído, evite nomes genéricos aqui — use em BOOST se quiser.
# =========================
MUST_HAVE_ANY = [
    # Institucional / poder público
    "câmara", "senado", "congresso", "plenário", "comissão",
    "ccj", "ccjc", "cft", "ctasp", "cdu", "cdc", "csau", "csauDE",
    "stf", "tcu", "cgu", "pgr", "mpf",

    # Fiscal / economia / tributos
    "imposto", "irpf", "imposto de renda", "tribut", "receita federal",
    "orçamento", "ldo", "ploa", "gastos", "lrf", "arcabouço", "inflação",
    "dívida", "juros", "selic", "banco central", "cmn", "pix",

    # SC / mandato (ajuste ao seu uso)
    "santa catarina", "sc", "florianópolis", "joinville", "chapecó",
    "julia zanatta", "zanatta",
    
    # Governo Federal
    "Lula", "Haddad", "Governo Federal", "Governo Lula", "Ministério da Fazenda",
    "Receita Federal", "Ministério da Justiça",
]

# Se aparecer no título/resumo -> bloqueia
BLOCKLIST = [
    "horóscopo", "bbb", "fofoca", "celebridade",
]

# Palavras que aumentam a prioridade (score) — não “liberam” sozinhas.
BOOST = [
    "câmara", "senado", "stf", "tcu",
    "orçamento", "ldo", "ploa", "irpf", "imposto", "receita federal",
    "zanatta", "santa catarina", "sc", "Lula", "Bolsonaro",
]

# =========================
# RECÊNCIA / LIMITES
# =========================
LOOKBACK_HOURS = 8          # padrão: últimas 8 horas (você pode mudar ou usar --hours)
MAX_ITEMS_PER_RUN = 20      # máximo por execução
SLEEP_BETWEEN_SENDS = 0.35  # intervalo entre envios
BRT = timezone(timedelta(hours=-3))  # Brasília

# =========================
# Persistência
# =========================
DB_PATH = "sent_items.db"
HIST_XLSX = "historico_news.xlsx"


# -------------------------
# Utils
# -------------------------
def normalize(s: str) -> str:
    return (s or "").strip()


def text_blob(title: str, summary: str) -> str:
    return f"{title}\n{summary}".lower()


def blocked(text: str) -> bool:
    return any(b.lower() in text for b in BLOCKLIST)


def passes_must_have(text: str) -> bool:
    if not MUST_HAVE_ANY:
        return True
    return any(k.lower() in text for k in MUST_HAVE_ANY)


def score_item(text: str) -> int:
    score = 0
    for b in BOOST:
        if b.lower() in text:
            score += 2
    return score


def brt_str(dt: datetime | None) -> str:
    if not dt:
        return ""
    return dt.astimezone(BRT).strftime("%d/%m/%Y %H:%M:%S")


def parse_published_dt(entry) -> datetime | None:
    """
    Extrai data de publicação do item RSS com fallback.
    Preferência:
      1) published_parsed/updated_parsed (struct_time)
      2) published/updated (string com formatos comuns)
    Retorna datetime em UTC, ou None se não achar.
    """
    # 1) Melhor caso
    for attr in ("published_parsed", "updated_parsed"):
        dt_struct = getattr(entry, attr, None)
        if dt_struct:
            return datetime(*dt_struct[:6], tzinfo=timezone.utc)

    # 2) Fallback por strings
    for attr in ("published", "updated"):
        s = getattr(entry, attr, None)
        if not s:
            continue
        s = str(s).strip()
        for fmt in (
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S%Z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                dt = datetime.strptime(s, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                pass

    return None


# -------------------------
# SQLite (deduplicação) + migração automática
# -------------------------
def init_db_and_migrate():
    """
    Garante tabela sent com colunas:
      id, title, link, source, published_at, sent_at
    Se banco antigo não tiver published_at, adiciona.
    """
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

    # Migração leve: garantir coluna published_at em bancos antigos
    cur.execute("PRAGMA table_info(sent)")
    cols = {row[1] for row in cur.fetchall()}
    if "published_at" not in cols:
        cur.execute("ALTER TABLE sent ADD COLUMN published_at TEXT")
        conn.commit()

    conn.close()


def was_sent(item_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sent WHERE id = ?", (item_id,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def mark_sent(item_id: str, title: str, link: str, source: str, published_at: str | None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO sent (id, title, link, source, published_at, sent_at) VALUES (?, ?, ?, ?, ?, ?)",
        (item_id, title, link, source, published_at, datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    conn.close()


# -------------------------
# XLSX Histórico
# -------------------------
def init_history_xlsx(path: str = HIST_XLSX):
    """Cria o XLSX com cabeçalho se não existir."""
    if os.path.exists(path):
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "Historico"
    ws.append([
        "Enviado_em_BRT",
        "Publicado_em_BRT",
        "Fonte",
        "Titulo",
        "Link",
        "Score",
    ])
    wb.save(path)


def append_history_row(
    sent_brt: str,
    published_brt: str,
    source: str,
    title: str,
    link: str,
    score: int,
    path: str = HIST_XLSX
):
    """Anexa uma linha no XLSX."""
    if not os.path.exists(path):
        init_history_xlsx(path)
    wb = load_workbook(path)
    ws = wb["Historico"] if "Historico" in wb.sheetnames else wb.active
    ws.append([sent_brt, published_brt, source, title, link, score])
    wb.save(path)


# -------------------------
# Telegram
# -------------------------
def telegram_send_message(token: str, chat_id: str, text: str):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        # Sem card/preview gigante (mais limpo)
        "disable_web_page_preview": True,
        "parse_mode": "HTML",
    }
    r = requests.post(url, json=payload, timeout=25)
    if r.status_code != 200:
        print("Telegram error:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()


# -------------------------
# RSS Fetch (prioridade: MAIS RECENTE)
# -------------------------
def fetch_news(lookback_hours: int):
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=lookback_hours)

    items = []
    for feed_url in RSS_FEEDS:
        parsed = feedparser.parse(feed_url)
        source_title = normalize(getattr(parsed.feed, "title", "")) or feed_url

        for e in parsed.entries[:160]:
            title = normalize(getattr(e, "title", ""))
            link = normalize(getattr(e, "link", ""))
            summary = normalize(getattr(e, "summary", ""))

            if not title or not link:
                continue

            blob = text_blob(title, summary)

            if blocked(blob):
                continue

            if not passes_must_have(blob):
                continue

            published_dt = parse_published_dt(e)

            # Recência: se tiver data e estiver fora da janela, corta.
            if published_dt and published_dt < cutoff:
                continue

            guid = normalize(getattr(e, "id", "")) or normalize(getattr(e, "guid", ""))
            item_id = guid if guid else f"{source_title}::{link}"

            items.append({
                "id": item_id,
                "title": title,
                "link": link,
                "source": source_title,
                "published_dt": published_dt,
                "score": score_item(blob),
            })

    # Remove duplicados por link
    seen_links = set()
    unique = []
    for it in items:
        if it["link"] in seen_links:
            continue
        seen_links.add(it["link"])
        unique.append(it)

    # Ordenação principal: mais novo primeiro.
    # Se published_dt for None, cai pro fim (epoch).
    # Score desempata.
    def sort_key(it):
        dt = it["published_dt"] or datetime(1970, 1, 1, tzinfo=timezone.utc)
        return (dt, it["score"])

    unique.sort(key=sort_key, reverse=True)
    return unique


def format_message(item: dict) -> str:
    """
    Formato compacto (gabinete):
      📰 Título
      🏷 Fonte • 04/01 17:22 (BRT)
      🔗 Abrir
    """
    title = html.escape(item["title"])
    source = html.escape(item["source"])
    link = item["link"].replace('"', "%22")

    if item.get("published_dt"):
        when = item["published_dt"].astimezone(BRT).strftime("%d/%m %H:%M")
        meta = f"{source} • {when} (BRT)"
    else:
        meta = f"{source} • (sem horário no RSS)"

    return (
        f"📰 <b>{title}</b>\n"
        f"🏷 <i>{meta}</i>\n"
        f"🔗 <a href=\"{link}\">Abrir</a>"
    )


# -------------------------
# Execução
# -------------------------
def run(lookback_hours: int):
    if not TELEGRAM_BOT_TOKEN:
        raise SystemExit("Erro: defina TELEGRAM_BOT_TOKEN (variável de ambiente).")
    if not TELEGRAM_CHAT_ID:
        raise SystemExit("Erro: defina TELEGRAM_CHAT_ID (variável de ambiente).")

    init_db_and_migrate()
    init_history_xlsx()

    news = fetch_news(lookback_hours)

    sent_count = 0
    for item in news:
        if sent_count >= MAX_ITEMS_PER_RUN:
            break
        if was_sent(item["id"]):
            continue

        # Envia
        telegram_send_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, format_message(item))

        # Histórico XLSX
        sent_brt = datetime.now(timezone.utc).astimezone(BRT).strftime("%d/%m/%Y %H:%M:%S")
        published_brt = brt_str(item.get("published_dt"))
        append_history_row(
            sent_brt=sent_brt,
            published_brt=published_brt,
            source=item["source"],
            title=item["title"],
            link=item["link"],
            score=int(item.get("score", 0)),
        )

        # Dedup DB
        published_iso = item["published_dt"].isoformat() if item.get("published_dt") else None
        mark_sent(item["id"], item["title"], item["link"], item["source"], published_iso)

        sent_count += 1
        time.sleep(SLEEP_BETWEEN_SENDS)

    print(f"OK: enviadas {sent_count} notícias.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=None, help="Janela de recência em horas (ex: 4, 6, 12, 24)")
    args = parser.parse_args()

    hours = LOOKBACK_HOURS
    if args.hours is not None:
        hours = max(1, int(args.hours))

    run(hours)


if __name__ == "__main__":
    main()
