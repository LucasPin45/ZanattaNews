"""
telegram_news_bot.py

Monitor Parlamentar Zanatta - Versão Otimizada
==============================================
- Coleta notícias políticas/institucionais via RSS (paralelo)
- Prioriza por relevância (contagem de keywords) + recência
- Deduplicação inteligente (similaridade de títulos)
- Configuração externa via YAML
- Logging estruturado
- Retry automático em falhas de rede
- Resumo diário opcional
- Histórico em SQLite + XLSX
- GitHub Actions (cron 15 min)
"""

import os
import sys
import time
import sqlite3
import html
import logging
import argparse
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import feedparser
from openpyxl import Workbook, load_workbook


# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURAÇÃO
# ============================================================
def load_config(config_path: str = "config.yaml") -> dict:
    """Carrega configuração do arquivo YAML."""
    path = Path(config_path)
    if not path.exists():
        logger.error(f"Arquivo de configuração não encontrado: {config_path}")
        raise SystemExit(1)
    
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ============================================================
# TELEGRAM (via Secrets / variáveis de ambiente)
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID nas variáveis de ambiente.")

BRT = timezone(timedelta(hours=-3))


# ============================================================
# HTTP SESSION COM RETRY
# ============================================================
def create_session() -> requests.Session:
    """Cria sessão HTTP com retry automático."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


SESSION = create_session()


# ============================================================
# BANCO (deduplicação)
# ============================================================
def init_db(db_path: str):
    """Inicializa banco SQLite com índices."""
    conn = sqlite3.connect(db_path)
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
    # Índice para buscas mais rápidas
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sent_published ON sent(published_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sent_sent_at ON sent(sent_at)")
    conn.commit()
    conn.close()
    logger.debug(f"Banco inicializado: {db_path}")


def was_sent(db_path: str, item_id: str) -> bool:
    """Verifica se item já foi enviado."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sent WHERE id=?", (item_id,))
    r = cur.fetchone()
    conn.close()
    return r is not None


def mark_sent(db_path: str, item: dict):
    """Marca item como enviado."""
    conn = sqlite3.connect(db_path)
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


def get_sent_titles_today(db_path: str) -> list[str]:
    """Retorna títulos enviados hoje (para deduplicação por similaridade)."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0).isoformat()
    cur.execute("SELECT title FROM sent WHERE sent_at >= ?", (today_start,))
    titles = [row[0] for row in cur.fetchall()]
    conn.close()
    return titles


def count_sent_today(db_path: str) -> int:
    """Conta notícias enviadas hoje."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    today = datetime.now(BRT).strftime("%Y-%m-%d")
    cur.execute("SELECT COUNT(*) FROM sent WHERE DATE(sent_at) = ?", (today,))
    count = cur.fetchone()[0]
    conn.close()
    return count


# ============================================================
# HISTÓRICO XLSX
# ============================================================
def init_xlsx(xlsx_path: str):
    """Inicializa arquivo Excel de histórico."""
    if os.path.exists(xlsx_path):
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "Historico"
    ws.append([
        "Enviado_BRT", "Publicado_BRT", "Fonte",
        "Titulo", "Link", "Keyword", "Contexto", "Relevancia"
    ])
    wb.save(xlsx_path)
    logger.debug(f"XLSX inicializado: {xlsx_path}")


def append_xlsx(xlsx_path: str, row: list):
    """Adiciona linha ao histórico Excel."""
    wb = load_workbook(xlsx_path)
    ws = wb.active
    ws.append(row)
    wb.save(xlsx_path)


# ============================================================
# TELEGRAM
# ============================================================
def send_telegram(msg: str) -> bool:
    """Envia mensagem para o Telegram com retry."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = SESSION.post(
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
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao enviar Telegram: {e}")
        return False


def send_daily_summary(db_path: str):
    """Envia resumo diário de notícias."""
    count = count_sent_today(db_path)
    msg = (
        f"📊 <b>Resumo do dia</b>\n"
        f"📰 Notícias enviadas hoje: <b>{count}</b>\n"
        f"🕐 {datetime.now(BRT).strftime('%d/%m/%Y %H:%M')} (BRT)"
    )
    if send_telegram(msg):
        logger.info(f"Resumo diário enviado: {count} notícias")


# ============================================================
# AUXILIARES
# ============================================================
def normalize(s: str) -> str:
    """Normaliza string removendo espaços extras."""
    return (s or "").strip()


def parse_published_dt(entry) -> datetime | None:
    """Extrai datetime de publicação do entry RSS."""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        return datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
    return None


def find_context(summary: str, keywords: list[str], context_chars: int, max_len: int) -> tuple[str, str]:
    """Encontra contexto ao redor da keyword no texto."""
    raw = normalize(summary)
    low = raw.lower()

    for kw in keywords:
        k = kw.lower()
        idx = low.find(k)
        if idx != -1:
            start = max(0, idx - context_chars)
            end = min(len(raw), idx + len(k) + context_chars)
            ctx = raw[start:end]
            
            # Destaca keyword
            ctx_low = ctx.lower()
            kw_idx = ctx_low.find(k)
            if kw_idx != -1:
                ctx = ctx[:kw_idx] + f"<b>{ctx[kw_idx:kw_idx+len(k)]}</b>" + ctx[kw_idx+len(k):]
            
            if len(ctx) > max_len:
                ctx = ctx[:max_len] + "…"
            return kw, html.escape(ctx, quote=False).replace("&lt;b&gt;", "<b>").replace("&lt;/b&gt;", "</b>")
    return "", ""


def count_keywords(text: str, keywords: list[str]) -> int:
    """Conta quantas keywords aparecem no texto."""
    text_lower = text.lower()
    return sum(1 for k in keywords if k.lower() in text_lower)


def is_similar(title1: str, title2: str, threshold: float = 0.85) -> bool:
    """Verifica se dois títulos são similares."""
    return SequenceMatcher(None, title1.lower(), title2.lower()).ratio() > threshold


def is_duplicate_by_similarity(title: str, existing_titles: list[str], threshold: float) -> bool:
    """Verifica se título é similar a algum já enviado."""
    for existing in existing_titles:
        if is_similar(title, existing, threshold):
            return True
    return False


# ============================================================
# FETCH PARALELO
# ============================================================
def fetch_feed(url: str) -> tuple[str, list, str | None]:
    """Busca um feed RSS. Retorna (url, entries, source_name)."""
    try:
        parsed = feedparser.parse(url)
        source = parsed.feed.title if hasattr(parsed.feed, "title") else url
        return (url, parsed.entries[:120], source)
    except Exception as e:
        logger.warning(f"Erro no feed {url}: {e}")
        return (url, [], None)


def fetch_all_feeds(feeds: list[str], workers: int) -> list[tuple[str, list, str]]:
    """Busca todos os feeds em paralelo."""
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_feed, url): url for url in feeds}
        for future in as_completed(futures):
            url = futures[future]
            try:
                result = future.result()
                if result[1]:  # Se tem entries
                    results.append(result)
            except Exception as e:
                logger.warning(f"Exceção no feed {url}: {e}")
    return results


# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================
def run(config: dict, hours_override: int | None = None, send_summary: bool = False):
    """Executa o bot."""
    
    settings = config["settings"]
    files = config["files"]
    
    db_path = files["database"]
    xlsx_path = files["history_xlsx"]
    
    lookback_hours = hours_override or settings["lookback_hours"]
    max_items = settings["max_items_per_run"]
    sleep_time = settings["sleep_between_sends"]
    context_chars = settings["context_chars"]
    max_context_len = settings["max_context_len"]
    similarity_threshold = settings["similarity_threshold"]
    parallel_workers = settings["parallel_workers"]
    
    keywords = config["keywords"]
    blocklist = config["blocklist"]
    feeds = config["feeds"]
    
    # Inicializa banco e xlsx
    init_db(db_path)
    init_xlsx(xlsx_path)
    
    # Verifica se deve enviar resumo diário
    if send_summary:
        send_daily_summary(db_path)
        return
    
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    logger.info(f"Buscando notícias das últimas {lookback_hours}h ({len(feeds)} feeds)")
    
    # Busca feeds em paralelo
    feed_results = fetch_all_feeds(feeds, parallel_workers)
    logger.info(f"Feeds processados: {len(feed_results)}/{len(feeds)}")
    
    # Processa entries
    items = []
    for url, entries, source in feed_results:
        for e in entries:
            title = normalize(getattr(e, "title", ""))
            link = normalize(getattr(e, "link", ""))
            summary = normalize(getattr(e, "summary", ""))

            if not title or not link:
                continue

            blob = f"{title}\n{summary}".lower()
            
            # Verifica keywords
            if not any(k.lower() in blob for k in keywords):
                continue
            
            # Verifica blocklist
            if any(b.lower() in blob for b in blocklist):
                continue

            pub_dt = parse_published_dt(e)
            if pub_dt and pub_dt < cutoff:
                continue

            kw, ctx = find_context(summary, keywords, context_chars, max_context_len)
            relevance = count_keywords(blob, keywords)

            items.append({
                "id": getattr(e, "id", link),
                "title": title,
                "link": link,
                "source": source,
                "published_at": pub_dt.isoformat() if pub_dt else None,
                "published_brt": pub_dt.astimezone(BRT).strftime("%d/%m %H:%M") if pub_dt else "",
                "kw": kw,
                "ctx": ctx,
                "relevance": relevance,
            })

    logger.info(f"Itens filtrados: {len(items)}")
    
    # Ordena por relevância (mais keywords) e depois por recência
    items.sort(key=lambda x: (x["relevance"], x["published_at"] or ""), reverse=True)

    # Carrega títulos já enviados para deduplicação por similaridade
    sent_titles = get_sent_titles_today(db_path)
    
    sent = 0
    skipped_similar = 0
    skipped_duplicate = 0
    
    for it in items:
        if sent >= max_items:
            break
        
        # Deduplicação por ID
        if was_sent(db_path, it["id"]):
            skipped_duplicate += 1
            continue
        
        # Deduplicação por similaridade de título
        if is_duplicate_by_similarity(it["title"], sent_titles, similarity_threshold):
            skipped_similar += 1
            logger.debug(f"Título similar ignorado: {it['title'][:50]}...")
            continue

        msg = (
            f"📰 <b>{html.escape(it['title'])}</b>\n"
            f"🏷 <i>{html.escape(it['source'])} • {it['published_brt']} (BRT)</i>\n"
            f"🔎 <b>Gatilho:</b> <code>{html.escape(it['kw'])}</code>\n"
            f"🧾 <i>{it['ctx']}</i>\n"
            f"🔗 {it['link']}"
        )

        if send_telegram(msg):
            append_xlsx(xlsx_path, [
                datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S"),
                it["published_brt"],
                it["source"],
                it["title"],
                it["link"],
                it["kw"],
                it["ctx"],
                it["relevance"],
            ])

            mark_sent(db_path, it)
            sent_titles.append(it["title"])  # Adiciona para verificação
            sent += 1
            time.sleep(sleep_time)
        else:
            logger.warning(f"Falha ao enviar: {it['title'][:50]}...")

    logger.info(
        f"Concluído: {sent} enviadas, "
        f"{skipped_duplicate} duplicadas (ID), "
        f"{skipped_similar} similares ignoradas"
    )


def main():
    parser = argparse.ArgumentParser(description="Telegram News Bot")
    parser.add_argument("--hours", type=int, help="Override lookback hours")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--summary", action="store_true", help="Send daily summary only")
    args = parser.parse_args()
    
    config = load_config(args.config)
    run(config, hours_override=args.hours, send_summary=args.summary)


if __name__ == "__main__":
    main()