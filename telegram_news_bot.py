"""
telegram_news_bot.py

Monitor Parlamentar Zanatta - versão com melhorias
===================================================
FONTES:
- RSS de portais de notícias
- Google Alerts (via RSS)
- Pauta da Câmara dos Deputados (API Dados Abertos)
- Diário Oficial da União (scraping)

MELHORIAS DESTA VERSÃO (ver MELHORIAS.md):
- Corrige a ordem das colunas gravadas no banco (e migra as linhas antigas),
  o que conserta o resumo diário
- DOU: id estável (hashlib), leitura do JSON da página, alerta se o layout mudar
- Planilha gravada UMA vez por rodada; banco podado (padrão: 90 dias)
- Palavras-chave por palavra inteira, com nível de prioridade (Zanatta primeiro)
- Contexto sem HTML; estilo de mensagem "completa" ou "whatsapp"
"""

import os
import sys
import re
import time
import json
import sqlite3
import html
import hashlib
import logging
import argparse
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote, urljoin
from typing import Optional

import yaml
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import feedparser
from openpyxl import Workbook, load_workbook
from bs4 import BeautifulSoup


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
# TELEGRAM
# ============================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID nas variáveis de ambiente.")

BRT = timezone(timedelta(hours=-3))
TELEGRAM_MAX = 4096
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


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
    session.headers.update({"User-Agent": USER_AGENT})
    return session


SESSION = create_session()


# ============================================================
# BANCO DE DADOS
# ============================================================
KNOWN_TYPES = ("rss", "google_alert", "camara", "dou", "alerta")
_CONNS: dict = {}


def get_conn(db_path: str) -> sqlite3.Connection:
    """Reaproveita a mesma conexão durante a execução (antes abria uma por consulta)."""
    conn = _CONNS.get(db_path)
    if conn is None:
        conn = sqlite3.connect(db_path)
        _CONNS[db_path] = conn
    return conn


def close_conns():
    for c in _CONNS.values():
        try:
            c.close()
        except Exception:
            pass
    _CONNS.clear()


def fix_shifted_columns(conn: sqlite3.Connection) -> int:
    """
    Corrige linhas gravadas com as colunas trocadas.

    A versão anterior fazia INSERT posicional em (id, title, link, source,
    source_type, published_at, sent_at), mas a tabela tem a ordem
    (..., published_at, sent_at, source_type). Resultado: published_at guardava
    o tipo, sent_at guardava a data de publicação e source_type guardava a data
    de envio. Linhas assim têm published_at igual a um tipo conhecido.
    O UPDATE usa os valores antigos do lado direito, então a rotação é segura
    e idempotente (depois dela nenhuma linha casa mais com o filtro).
    """
    cur = conn.execute(
        """
        UPDATE sent
           SET published_at = sent_at,
               sent_at      = source_type,
               source_type  = published_at
         WHERE published_at IN ('rss', 'google_alert', 'camara', 'dou')
        """
    )
    conn.commit()
    if cur.rowcount:
        logger.info(f"Migração: {cur.rowcount} linhas com colunas trocadas foram corrigidas")
    return cur.rowcount


def prune_db(conn: sqlite3.Connection, keep_days: Optional[int]) -> int:
    """Apaga registros antigos (o feed só olha as últimas horas, 90 dias sobra)."""
    if not keep_days:
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).isoformat()
    cur = conn.execute(
        "DELETE FROM sent WHERE sent_at IS NOT NULL AND sent_at < ?", (cutoff,)
    )
    deleted = cur.rowcount
    conn.commit()
    if deleted:
        logger.info(f"Poda: {deleted} registros com mais de {keep_days} dias removidos")
    if deleted > 500:
        conn.execute("VACUUM")  # devolve o espaço ao arquivo
    return deleted


def init_db(db_path: str, keep_days: Optional[int] = None):
    """Inicializa banco SQLite, migra o esquema e corrige/poda dados."""
    conn = get_conn(db_path)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS sent (
            id TEXT PRIMARY KEY,
            title TEXT,
            link TEXT,
            source TEXT,
            published_at TEXT,
            sent_at TEXT
        )
    """)

    columns = [col[1] for col in conn.execute("PRAGMA table_info(sent)").fetchall()]
    if "source_type" not in columns:
        conn.execute("ALTER TABLE sent ADD COLUMN source_type TEXT DEFAULT 'rss'")
        logger.info("Migração: coluna source_type adicionada ao banco")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_sent_published ON sent(published_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sent_sent_at ON sent(sent_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sent_source_type ON sent(source_type)")
    conn.commit()

    fix_shifted_columns(conn)
    prune_db(conn, keep_days)


def was_sent(db_path: str, item_id: str) -> bool:
    cur = get_conn(db_path).execute("SELECT 1 FROM sent WHERE id=?", (item_id,))
    return cur.fetchone() is not None


def mark_sent(db_path: str, item: dict):
    conn = get_conn(db_path)
    # colunas nomeadas: a ordem dos valores não depende mais da ordem da tabela
    conn.execute(
        "INSERT OR IGNORE INTO sent "
        "(id, title, link, source, source_type, published_at, sent_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            item["id"],
            item["title"],
            item["link"],
            item["source"],
            item.get("source_type", "rss"),
            item.get("published_at"),
            datetime.now(timezone.utc).isoformat(),
        )
    )
    conn.commit()


def get_recent_titles(db_path: str, hours: int = 24) -> list[str]:
    """Títulos enviados nas últimas horas (usados na checagem de similaridade)."""
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    cur = get_conn(db_path).execute("SELECT title FROM sent WHERE sent_at >= ?", (since,))
    return [row[0] for row in cur.fetchall() if row[0]]


def count_sent_today(db_path: str) -> dict:
    """Conta o que foi enviado hoje (dia de Brasília) por tipo."""
    start_brt = datetime.now(BRT).replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = start_brt.astimezone(timezone.utc).isoformat()
    cur = get_conn(db_path).execute(
        "SELECT source_type, COUNT(*) FROM sent WHERE sent_at >= ? GROUP BY source_type",
        (start_utc,),
    )
    return dict(cur.fetchall())


# ============================================================
# HISTÓRICO XLSX
# ============================================================
XLSX_HEADER = [
    "Enviado_BRT", "Publicado_BRT", "Fonte", "Tipo",
    "Titulo", "Link", "Keyword", "Contexto", "Relevancia"
]
XLSX_ROTATE_MB = 40


def rotate_xlsx_if_big(xlsx_path: str):
    """Se a planilha passar de ~40 MB, guarda como arquivo datado e recomeça."""
    p = Path(xlsx_path)
    if p.exists() and p.stat().st_size > XLSX_ROTATE_MB * 1024 * 1024:
        novo = p.with_name(f"{p.stem}_{datetime.now(BRT):%Y%m%d}{p.suffix}")
        p.rename(novo)
        logger.info(f"Planilha grande demais: arquivada como {novo.name}")


def init_xlsx(xlsx_path: str):
    if os.path.exists(xlsx_path):
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "Historico"
    ws.append(XLSX_HEADER)
    wb.save(xlsx_path)


def append_xlsx_rows(xlsx_path: str, rows: list):
    """Abre e salva a planilha uma única vez por rodada (antes era a cada notícia)."""
    if not rows:
        return
    wb = load_workbook(xlsx_path)
    ws = wb.active
    for row in rows:
        ws.append(row)
    wb.save(xlsx_path)
    logger.info(f"Planilha: {len(rows)} linhas adicionadas")


# ============================================================
# TELEGRAM
# ============================================================
def send_telegram(msg: str) -> bool:
    """Envia mensagem para o Telegram."""
    if len(msg) > TELEGRAM_MAX:
        # corta numa quebra de linha (as tags HTML nunca atravessam linhas)
        cut = msg.rfind("\n", 0, TELEGRAM_MAX - 100)
        msg = msg[: cut if cut > 0 else TELEGRAM_MAX - 100] + "\n…"
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
        detalhe = ""
        if getattr(e, "response", None) is not None:
            detalhe = f" | {e.response.text[:200]}"
        logger.error(f"Erro ao enviar Telegram: {e}{detalhe}")
        return False


TYPE_EMOJI = {"rss": "📰", "google_alert": "🔔", "camara": "🏛️", "dou": "📜", "alerta": "⚠️"}
TYPE_NAME = {"rss": "Notícias", "google_alert": "Google Alerts", "camara": "Câmara",
             "dou": "DOU", "alerta": "Alertas do bot"}


def send_daily_summary(db_path: str):
    """Envia resumo diário."""
    counts = count_sent_today(db_path)
    total = sum(counts.values())

    breakdown = []
    for t, c in sorted(counts.items(), key=lambda kv: kv[1], reverse=True):
        emoji = TYPE_EMOJI.get(t, "📌")
        name = TYPE_NAME.get(t, str(t))
        breakdown.append(f"{emoji} {html.escape(name)}: {c}")

    msg = (
        f"📊 <b>Resumo do dia</b>\n\n"
        f"📰 Total enviado: <b>{total}</b>\n\n"
        + "\n".join(breakdown) + "\n\n"
        f"🕐 {datetime.now(BRT).strftime('%d/%m/%Y %H:%M')} (BRT)"
    )

    if send_telegram(msg):
        logger.info(f"Resumo diário enviado: {total} itens")


# ============================================================
# PALAVRAS-CHAVE (palavra inteira + prioridade)
# ============================================================
class Keyword:
    """
    Palavra-chave casada como PALAVRA INTEIRA, sem diferenciar maiúsculas.
    Termine com * para casar também o começo de outras palavras
    (ex.: "tribut*" pega tributo, tributário, tributação).
    """
    __slots__ = ("text", "pattern", "priority")

    def __init__(self, text: str, priority: bool = False):
        raw = text.strip()
        prefix = raw.endswith("*")
        base = raw.rstrip("*").strip()
        core = re.escape(base).replace(r"\ ", r"\s+")
        self.text = base
        self.priority = priority
        self.pattern = re.compile(
            rf"(?<!\w){core}" + ("" if prefix else r"(?!\w)"), re.IGNORECASE
        )


def build_keywords(config: dict) -> list:
    """Prioritárias primeiro, depois as gerais (sem repetir)."""
    out, seen = [], set()
    for kw in config.get("priority_keywords") or []:
        if str(kw).strip():
            k = Keyword(str(kw), priority=True)
            out.append(k)
            seen.add(k.text.lower())
    for kw in config.get("keywords") or []:
        if not str(kw).strip():
            continue
        k = Keyword(str(kw), priority=False)
        if k.text.lower() in seen:
            continue
        out.append(k)
    return out


# ============================================================
# AUXILIARES
# ============================================================
def normalize(s: str) -> str:
    return (s or "").strip()


def clean_html(s: str) -> str:
    """Tira tags HTML e entidades de um resumo de RSS."""
    if not s:
        return ""
    if "<" in s:
        s = BeautifulSoup(s, "html.parser").get_text(" ", strip=True)
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()


def parse_published_dt(entry) -> Optional[datetime]:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        return datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
    return None


def find_context(title: str, summary: str, hits: list, context_chars: int, max_len: int) -> tuple:
    """Trecho do resumo em volta da palavra-chave, com ela em negrito (HTML do Telegram)."""
    for k in hits:  # hits já vem com as prioritárias na frente
        m = k.pattern.search(summary)
        if not m:
            continue
        start = max(0, m.start() - context_chars)
        end = min(len(summary), m.end() + context_chars)
        before, mid, after = summary[start:m.start()], m.group(0), summary[m.end():end]
        # garante o tamanho máximo cortando o fim, nunca o meio do <b>
        room = max_len - len(before) - len(mid)
        if room < len(after):
            after = after[: max(room, 0)]
            end = m.end() + len(after)
        ctx = (
            ("…" if start > 0 else "")
            + html.escape(before, quote=False)
            + "<b>" + html.escape(mid, quote=False) + "</b>"
            + html.escape(after, quote=False)
            + ("…" if end < len(summary) else "")
        )
        return k.text, ctx

    for k in hits:
        if k.pattern.search(title):
            return k.text, "(ver título)"
    return "", ""


def is_similar(title1: str, title2: str, threshold: float = 0.85) -> bool:
    sm = SequenceMatcher(None, title1.lower(), title2.lower())
    # limites baratos primeiro: se nem o teto passa do limiar, não precisa do ratio()
    if sm.real_quick_ratio() <= threshold or sm.quick_ratio() <= threshold:
        return False
    return sm.ratio() > threshold


def is_duplicate_by_similarity(title: str, existing_titles: list, threshold: float) -> bool:
    for existing in existing_titles:
        if is_similar(title, existing, threshold):
            return True
    return False


# ============================================================
# FETCH RSS (PARALELO)
# ============================================================
def fetch_feed(url: str) -> tuple:
    try:
        r = SESSION.get(url, timeout=20)  # com timeout (o feedparser sozinho não tem)
        r.raise_for_status()
        parsed = feedparser.parse(r.content)
        source = parsed.feed.get("title") or url
        return (url, parsed.entries[:120], source)
    except Exception as e:
        logger.warning(f"Erro no feed {url}: {e}")
        return (url, [], None)


def fetch_all_feeds(feeds: list, workers: int) -> list:
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_feed, url): url for url in feeds}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result[1]:
                    results.append(result)
            except Exception as e:
                logger.warning(f"Exceção em feed: {e}")
    return results


# ============================================================
# CÂMARA DOS DEPUTADOS - PAUTA DO PLENÁRIO
# ============================================================
def fetch_camara_pauta() -> list:
    """Busca pauta do dia da Câmara via API."""
    items = []
    today = datetime.now(BRT).strftime("%Y-%m-%d")

    try:
        url = (
            "https://dadosabertos.camara.leg.br/api/v2/eventos"
            f"?dataInicio={today}&dataFim={today}&ordem=ASC&ordenarPor=dataHoraInicio"
        )
        r = SESSION.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()

        for evento in data.get("dados", []):
            orgaos = evento.get("orgaos", [])
            is_plenario = any("Plenário" in o.get("nome", "") for o in orgaos)
            if not is_plenario:
                continue

            titulo = evento.get("descricaoTipo", "Sessão")
            descricao = evento.get("descricao", "")
            data_hora = evento.get("dataHoraInicio", "")
            situacao = evento.get("descricaoSituacao", "")

            evento_id = evento.get("id")
            pauta_items = []

            if evento_id:
                try:
                    pauta_url = f"https://dadosabertos.camara.leg.br/api/v2/eventos/{evento_id}/pauta"
                    pr = SESSION.get(pauta_url, timeout=15)
                    if pr.status_code == 200:
                        pauta_data = pr.json()
                        for p in pauta_data.get("dados", [])[:5]:
                            prop = p.get("proposicao_", {})
                            if prop:
                                pauta_items.append(
                                    f"• {prop.get('siglaTipo', '')} {prop.get('numero', '')}/{prop.get('ano', '')}"
                                )
                except Exception:
                    pass

            hora_fmt = ""
            if data_hora:
                try:
                    dt = datetime.fromisoformat(data_hora.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=BRT)
                    hora_fmt = dt.astimezone(BRT).strftime("%H:%M")
                except Exception:
                    hora_fmt = data_hora

            items.append({
                "id": f"camara_{evento_id}",
                "title": f"{titulo}: {descricao}" if descricao else titulo,
                "link": f"https://www.camara.leg.br/evento-legislativo/{evento_id}" if evento_id else "https://www.camara.leg.br/agenda",
                "source": "Câmara dos Deputados",
                "source_type": "camara",
                "published_at": data_hora,
                "hora": hora_fmt,
                "situacao": situacao,
                "pauta": pauta_items,
            })

        logger.info(f"Câmara: {len(items)} eventos encontrados")

    except Exception as e:
        logger.error(f"Erro ao buscar pauta da Câmara: {e}")

    return items


def send_camara_pauta(db_path: str):
    """Envia pauta do dia da Câmara."""
    pauta_id = f"camara_pauta_{datetime.now(BRT).strftime('%Y%m%d')}"
    if was_sent(db_path, pauta_id):
        logger.info("Pauta da Câmara já enviada hoje")
        return

    eventos = fetch_camara_pauta()

    if not eventos:
        logger.info("Câmara: nenhum evento no plenário hoje")
        return

    msg_parts = ["🏛️ <b>PAUTA DO PLENÁRIO - CÂMARA</b>\n"]
    msg_parts.append(f"📅 {datetime.now(BRT).strftime('%d/%m/%Y')}\n")

    for ev in eventos:
        msg_parts.append(f"\n⏰ <b>{ev['hora']}</b> - {html.escape(ev['title'])}")
        if ev['situacao']:
            msg_parts.append(f"   📌 {html.escape(ev['situacao'])}")
        if ev['pauta']:
            msg_parts.append("   📋 Pauta:")
            for p in ev['pauta']:
                msg_parts.append(f"      {html.escape(p)}")
        msg_parts.append(f"   🔗 {ev['link']}")

    msg = "\n".join(msg_parts)

    if send_telegram(msg):
        mark_sent(db_path, {
            "id": pauta_id,
            "title": "Pauta do Plenário",
            "link": "https://www.camara.leg.br/agenda",
            "source": "Câmara dos Deputados",
            "source_type": "camara",
            "published_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.info("Pauta da Câmara enviada com sucesso")


# ============================================================
# DIÁRIO OFICIAL DA UNIÃO (DOU)
# ============================================================
def _dou_id(secao: str, title: str, link: str) -> str:
    # hash() do Python muda a cada execução; sha1 é estável entre execuções
    digest = hashlib.sha1(f"{secao}|{title}|{link}".encode("utf-8")).hexdigest()[:20]
    return f"dou_{secao}_{digest}"


def parse_dou_page(page_html: str, secao: str, keyword: str) -> tuple:
    """
    Lê uma página de resultados do DOU. Retorna (itens, layout_reconhecido).

    1) tenta o JSON embutido na página (<script id="params"> -> jsonArray);
    2) se não houver, tenta os seletores CSS antigos.
    "layout_reconhecido" = False sinaliza que o site provavelmente mudou.
    """
    soup = BeautifulSoup(page_html, "html.parser")
    items = []
    recognized = False

    script = soup.find("script", id="params")
    if script is not None:
        try:
            data = json.loads(script.string or script.get_text() or "{}")
            arr = data.get("jsonArray")
            if isinstance(arr, list):
                recognized = True
                for a in arr[:10]:
                    title = clean_html(str(a.get("title") or ""))
                    if not title:
                        continue
                    url_title = a.get("urlTitle") or ""
                    link = f"https://www.in.gov.br/web/dou/-/{url_title}" if url_title else ""
                    summary = clean_html(str(a.get("content") or a.get("abstract") or ""))
                    orgao = clean_html(str(a.get("hierarchyStr") or a.get("pubName") or "DOU"))[:80]
                    items.append((title, link, summary, orgao))
        except (ValueError, AttributeError, TypeError):
            pass

    if not items:
        results = soup.select(".resultados-dou .resultado-item") or soup.select(".results-list .item")
        if results or soup.select_one(".resultados-dou, .results-list"):
            recognized = True
        for item in results[:10]:
            title_el = item.select_one("h5, .title, a.title-content")
            link_el = item.select_one("a[href*='/web/dou/']")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            link = urljoin("https://www.in.gov.br", link_el.get("href", "")) if link_el else ""
            summary_el = item.select_one(".abstract, .resumo, p")
            summary = clean_html(summary_el.get_text(" ", strip=True)) if summary_el else ""
            org_el = item.select_one(".orgao, .organization")
            orgao = org_el.get_text(strip=True) if org_el else "DOU"
            items.append((title, link, summary, orgao))

    out = []
    now_brt = datetime.now(BRT).strftime("%d/%m %H:%M")
    for title, link, summary, orgao in items:
        out.append({
            "id": _dou_id(secao, title, link),
            "title": title,
            "link": link or f"https://www.in.gov.br/consulta/-/buscar/dou?q={quote(keyword)}",
            "summary": summary,
            "source": f"DOU Seção {secao} - {orgao}",
            "source_type": "dou",
            "published_at": datetime.now(timezone.utc).isoformat(),
            "published_brt": now_brt,
            "kw": keyword,
        })
    return out, recognized


def fetch_dou(keywords: list, secoes: list) -> tuple:
    """
    Busca publicações no DOU. Retorna (itens, status), com status em:
    "ok" | "layout" (o site respondeu, mas o formato não foi reconhecido) |
    "http" (nenhuma consulta funcionou).
    """
    items = []
    today = datetime.now(BRT).strftime("%d-%m-%Y")
    n_req = n_ok = n_recognized = 0

    for keyword in keywords:
        for secao in secoes:
            n_req += 1
            try:
                r = SESSION.get(
                    "https://www.in.gov.br/consulta/-/buscar/dou",
                    params={
                        "q": keyword, "s": secao,
                        "exactDate": "personalizado",
                        "publishFrom": today, "publishTo": today,
                        "delta": 20, "sortType": "0",
                    },
                    timeout=30,
                )
                if r.status_code != 200:
                    continue
                n_ok += 1
                found, recognized = parse_dou_page(r.text, secao, keyword)
                if recognized:
                    n_recognized += 1
                items.extend(found)
            except Exception as e:
                logger.warning(f"Erro ao buscar DOU para '{keyword}': {e}")

    seen, unique = set(), []
    for it in items:
        if it["id"] not in seen:
            seen.add(it["id"])
            unique.append(it)

    if n_req and n_ok == 0:
        status = "http"
    elif n_ok and n_recognized == 0:
        status = "layout"
    else:
        status = "ok"

    logger.info(
        f"DOU: {len(unique)} publicações | consultas={n_req} respondidas={n_ok} "
        f"layout_reconhecido={n_recognized} | status={status}"
    )
    return unique, status


def alert_dou_failure(db_path: str, status: str):
    """Avisa no Telegram (no máximo uma vez por dia) que o DOU não está funcionando."""
    alert_id = f"dou_alerta_{datetime.now(BRT):%Y%m%d}"
    if was_sent(db_path, alert_id):
        return
    motivo = (
        "o site do DOU não respondeu"
        if status == "http"
        else "o site do DOU respondeu, mas o formato da página não foi reconhecido (o layout pode ter mudado)"
    )
    if send_telegram(f"⚠️ <b>DOU sem resultados</b>\n{html.escape(motivo)}.\nA busca no Diário Oficial precisa de revisão."):
        mark_sent(db_path, {
            "id": alert_id, "title": "Alerta DOU", "link": "", "source": "Newsbot",
            "source_type": "alerta", "published_at": datetime.now(timezone.utc).isoformat(),
        })


# ============================================================
# PROCESSAMENTO DE RSS E GOOGLE ALERTS
# ============================================================
def process_rss_items(feed_results: list, keywords: list, blocklist: list,
                      cutoff: datetime, context_chars: int, max_context_len: int,
                      source_type: str = "rss") -> list:
    """Processa itens de feeds RSS."""
    items = []
    block = [b.lower() for b in blocklist]

    for url, entries, source in feed_results:
        for e in entries:
            title = clean_html(normalize(getattr(e, "title", "")))
            link = normalize(getattr(e, "link", ""))
            summary = clean_html(normalize(getattr(e, "summary", "")))

            if not title or not link:
                continue

            blob = f"{title}\n{summary}"
            hits = [k for k in keywords if k.pattern.search(blob)]
            if not hits:
                continue

            low = blob.lower()
            if any(b in low for b in block):
                continue

            pub_dt = parse_published_dt(e)
            if pub_dt and pub_dt < cutoff:
                continue

            priority = any(k.priority for k in hits)
            kw, ctx = find_context(title, summary, hits, context_chars, max_context_len)
            relevance = len(hits) + (100 if priority else 0)

            items.append({
                "id": getattr(e, "id", link),
                "title": title,
                "link": link,
                "source": source or url,
                "source_type": source_type,
                "published_at": pub_dt.isoformat() if pub_dt else None,
                "published_brt": pub_dt.astimezone(BRT).strftime("%d/%m %H:%M") if pub_dt else "",
                "kw": kw,
                "ctx": ctx,
                "relevance": relevance,
                "priority": priority,
            })

    return items


# ============================================================
# FORMATO DA MENSAGEM
# ============================================================
def format_message(it: dict, style: str = "completa") -> str:
    title = html.escape(it["title"])
    source = html.escape(it["source"])
    when = it.get("published_brt", "")
    priority = it.get("priority", False)

    if style == "whatsapp":
        # pronta para copiar e colar: título em negrito do WhatsApp, fonte/data e link
        meta = f"{source} • {when}" if when else source
        lead = "🚨 " if priority else ""
        return f"{lead}*{title}*\n{meta}\n{it['link']}"

    emoji = "🚨" if priority else TYPE_EMOJI.get(it.get("source_type", "rss"), "📰")
    return (
        f"{emoji} <b>{title}</b>\n"
        f"🏷 <i>{source} • {when} (BRT)</i>\n"
        f"🔎 <b>Gatilho:</b> <code>{html.escape(it.get('kw', ''))}</code>\n"
        f"🧾 <i>{it.get('ctx', '')}</i>\n"
        f"🔗 {it['link']}"
    )


# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================
def run(config: dict, hours_override: int = None, send_summary: bool = False,
        pauta_only: bool = False, dou_only: bool = False):
    """Executa o bot."""

    settings = config["settings"]
    files = config["files"]

    db_path = files["database"]
    xlsx_path = files["history_xlsx"]

    init_db(db_path, keep_days=settings.get("keep_days", 90))
    rotate_xlsx_if_big(xlsx_path)
    init_xlsx(xlsx_path)

    try:
        if send_summary:
            send_daily_summary(db_path)
            return

        if pauta_only:
            if config.get("camara_pauta", {}).get("enabled", False):
                send_camara_pauta(db_path)
            return

        lookback_hours = hours_override or settings["lookback_hours"]
        max_items = settings["max_items_per_run"]
        sleep_time = settings["sleep_between_sends"]
        context_chars = settings["context_chars"]
        max_context_len = settings["max_context_len"]
        similarity_threshold = settings["similarity_threshold"]
        parallel_workers = settings["parallel_workers"]
        style = settings.get("message_style", "completa")

        keywords = build_keywords(config)
        blocklist = config.get("blocklist") or []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

        all_items = []

        if not dou_only:
            # 1. RSS de notícias
            feeds = config.get("feeds", [])
            if feeds:
                logger.info(f"Buscando RSS ({len(feeds)} feeds)...")
                feed_results = fetch_all_feeds(feeds, parallel_workers)
                rss_items = process_rss_items(feed_results, keywords, blocklist, cutoff,
                                              context_chars, max_context_len, "rss")
                all_items.extend(rss_items)
                logger.info(f"RSS: {len(rss_items)} itens")

            # 2. Google Alerts
            ga_config = config.get("google_alerts", {})
            if ga_config.get("enabled", False) and ga_config.get("feeds"):
                logger.info("Buscando Google Alerts...")
                ga_results = fetch_all_feeds(ga_config["feeds"], parallel_workers)
                ga_items = process_rss_items(ga_results, keywords, blocklist, cutoff,
                                             context_chars, max_context_len, "google_alert")
                all_items.extend(ga_items)
                logger.info(f"Google Alerts: {len(ga_items)} itens")

        # 3. Diário Oficial da União (só nos horários de run_hours, ou sempre no modo --dou)
        dou_config = config.get("dou", {})
        if dou_config.get("enabled", False):
            run_hours = dou_config.get("run_hours")
            if dou_only or not run_hours or datetime.now(BRT).hour in run_hours:
                logger.info("Buscando DOU...")
                dou_items, dou_status = fetch_dou(
                    dou_config.get("keywords", []), dou_config.get("secoes", ["1"])
                )
                for item in dou_items:
                    # o resumo vai para dentro de HTML do Telegram: precisa de escape
                    item["ctx"] = html.escape(item.get("summary", "")[:max_context_len], quote=False)
                    item["relevance"] = 1
                    item["priority"] = False
                all_items.extend(dou_items)
                if dou_status != "ok" and dou_config.get("alert_on_failure", True):
                    alert_dou_failure(db_path, dou_status)

        # 4. Pauta da Câmara (na hora configurada)
        camara_config = config.get("camara_pauta", {})
        if not dou_only and camara_config.get("enabled", False):
            if datetime.now(BRT).hour == camara_config.get("send_hour", 7):
                send_camara_pauta(db_path)

        logger.info(f"Total de itens: {len(all_items)}")

        # prioritários primeiro; depois relevância; depois mais recentes
        all_items.sort(
            key=lambda x: (x.get("priority", False), x.get("relevance", 0), x.get("published_at") or ""),
            reverse=True,
        )

        sent_titles = get_recent_titles(db_path, hours=24)
        sent = sent_general = 0
        skipped_similar = skipped_duplicate = 0
        xlsx_rows = []

        try:
            for it in all_items:
                # o limite por rodada vale só para os itens gerais; prioritários sempre passam
                if not it.get("priority") and sent_general >= max_items:
                    break

                if was_sent(db_path, it["id"]):
                    skipped_duplicate += 1
                    continue

                if is_duplicate_by_similarity(it["title"], sent_titles, similarity_threshold):
                    skipped_similar += 1
                    continue

                if send_telegram(format_message(it, style)):
                    xlsx_rows.append([
                        datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S"),
                        it.get("published_brt", ""),
                        it["source"],
                        it.get("source_type", "rss"),
                        it["title"],
                        it["link"],
                        it.get("kw", ""),
                        it.get("ctx", ""),
                        it.get("relevance", 0),
                    ])
                    mark_sent(db_path, it)
                    sent_titles.append(it["title"])
                    sent += 1
                    if not it.get("priority"):
                        sent_general += 1
                    time.sleep(sleep_time)
        finally:
            # mesmo se algo falhar no meio, o que já foi enviado entra na planilha
            append_xlsx_rows(xlsx_path, xlsx_rows)

        logger.info(
            f"Concluído: {sent} enviadas, "
            f"{skipped_duplicate} duplicadas, "
            f"{skipped_similar} similares"
        )
    finally:
        close_conns()


def main():
    parser = argparse.ArgumentParser(description="Telegram News Bot - Monitor Parlamentar")
    parser.add_argument("--hours", type=int, help="Override lookback hours")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--summary", action="store_true", help="Send daily summary only")
    parser.add_argument("--pauta", action="store_true", help="Send Câmara pauta only")
    parser.add_argument("--dou", action="store_true", help="Run only the DOU search (diagnóstico)")
    args = parser.parse_args()

    config = load_config(args.config)
    run(config, hours_override=args.hours, send_summary=args.summary,
        pauta_only=args.pauta, dou_only=args.dou)


if __name__ == "__main__":
    main()
