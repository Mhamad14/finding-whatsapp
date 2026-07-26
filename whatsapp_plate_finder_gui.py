"""
whatsapp_plate_finder_gui.py
============================
Full web GUI for the WhatsApp PDF / plate-number finder.

  - Reuses the scraping engine (Playwright) and PDF extraction (pypdf).
  - SQLite index for fast offline search.
  - Browser UI served by Flask on http://localhost:5000

Designed to run on your machine now and easy to host later (just set
HOST/PORT env vars or put behind gunicorn/nginx).

Run:
    python whatsapp_plate_finder_gui.py
    # then open http://localhost:5000 in your browser
"""

from __future__ import annotations

import os
import re
import time
import json
import random
import io
import shutil
import sqlite3
import hashlib
import logging
import threading
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from flask import Flask, render_template_string, request, jsonify, send_file

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR    = Path(__file__).resolve().parent
SESSION_DIR = BASE_DIR / "wa_session"
PDF_DIR     = BASE_DIR / "downloaded_pdfs"
DB_PATH     = BASE_DIR / "whatsapp_index.db"

HEADLESS_DEFAULT        = False
DEFAULT_HISTORY_DAYS    = 365
INCREMENTAL_SYNC_DAYS   = 3

MESSAGE_LIST_SELECTORS = [
    'div[aria-label="Message list"]',
    'div[role="panel"] div[data-testid="conversation-panel-messages"]',
    'div[id="main"] div.copyable-area',
    '#main div[data-testid="conversation-panel-messages"]',
    '#main div[role="application"]',
]

PLATE_RE = re.compile(r"\b\d{6,7}\b")
VIN_RE   = re.compile(r"\b[A-HJ-NPR-Z0-9]{17}\b")

HOST = os.environ.get("WA_HOST", "127.0.0.1")
PORT = int(os.environ.get("WA_PORT", "5000"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-7s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("wa_gui")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
class Database:
    def __init__(self, path: Path = DB_PATH) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._create_schema()

    def _create_schema(self) -> None:
        with self._lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS pdfs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_name TEXT,
                    phone_number TEXT,
                    message_date TEXT,
                    pdf_filename TEXT UNIQUE,
                    local_file_path TEXT,
                    file_hash TEXT,
                    plates TEXT,
                    vins TEXT,
                    extracted INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now','localtime'))
                );
                CREATE TABLE IF NOT EXISTS sync_state (
                    chat_key TEXT PRIMARY KEY,
                    last_synced_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_pdfs_date    ON pdfs(message_date);
                CREATE INDEX IF NOT EXISTS idx_pdfs_phone   ON pdfs(phone_number);
                CREATE INDEX IF NOT EXISTS idx_pdfs_customer ON pdfs(customer_name);
                """
            )
            self.conn.commit()

    def upsert_pdf(self, customer_name, phone_number, message_date,
                   pdf_filename, local_file_path, file_hash):
        with self._lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO pdfs "
                "(customer_name, phone_number, message_date, pdf_filename, "
                " local_file_path, file_hash) VALUES (?,?,?,?,?,?)",
                (customer_name, phone_number, message_date,
                 pdf_filename, local_file_path, file_hash)
            )
            self.conn.commit()

    def mark_extracted(self, pdf_filename, plates, vins):
        with self._lock:
            self.conn.execute(
                "UPDATE pdfs SET plates=?, vins=?, extracted=1 WHERE pdf_filename=?",
                (json.dumps(plates), json.dumps(vins), pdf_filename)
            )
            self.conn.commit()

    def get_unextracted(self):
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM pdfs WHERE extracted=0 "
                "OR (extracted=1 AND (plates IS NULL OR plates='[]') "
                "AND (vins IS NULL OR vins='[]'))"
            ).fetchall()

    def get_all(self):
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM pdfs ORDER BY message_date DESC"
            ).fetchall()

    def search_by_plate(self, plate):
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM pdfs WHERE plates LIKE ?",
                (f'%"{plate}"%',)
            ).fetchall()

    def search_by_date(self, date_str):
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM pdfs WHERE substr(message_date,1,10)=? "
                "ORDER BY message_date DESC",
                (date_str,)
            ).fetchall()

    def search_by_customer(self, name):
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM pdfs WHERE customer_name LIKE ? "
                "ORDER BY message_date DESC",
                (f"%{name}%",)
            ).fetchall()

    def search_by_plate_and_date(self, plate="", date_str=""):
        """Search using either or both filters from the Search page."""
        conditions = []
        values = []
        if plate:
            conditions.append("plates LIKE ?")
            values.append(f'%"{plate}"%')
        if date_str:
            conditions.append("substr(message_date,1,10)=?")
            values.append(date_str)
        if not conditions:
            return []
        query = (
            "SELECT * FROM pdfs WHERE " + " AND ".join(conditions) +
            " ORDER BY message_date DESC"
        )
        with self._lock:
            return self.conn.execute(query, values).fetchall()

    def reset(self):
        """Clear indexed records but keep the database schema and login session."""
        with self._lock:
            self.conn.execute("DELETE FROM pdfs")
            self.conn.execute("DELETE FROM sync_state")
            self.conn.commit()

    def stats(self):
        with self._lock:
            total = self.conn.execute("SELECT COUNT(*) c FROM pdfs").fetchone()["c"]
            extracted = self.conn.execute(
                "SELECT COUNT(*) c FROM pdfs WHERE extracted=1"
            ).fetchone()["c"]
            with_plates = self.conn.execute(
                "SELECT COUNT(*) c FROM pdfs WHERE plates IS NOT NULL "
                "AND plates != '[]'"
            ).fetchone()["c"]
            return {"total": total, "extracted": extracted,
                    "with_plates": with_plates}

db = Database()

# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------
def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_text_from_pdf(path: Path) -> str:
    try:
        from pypdf import PdfReader
    except ImportError:
        return ""
    try:
        reader = PdfReader(str(path))
        parts = []
        for page in reader.pages:
            try:
                parts.append(page.extract_text() or "")
            except Exception:
                continue
        text = "\n".join(parts)
        # Scanned/image-only PDFs have little or no embedded text. Use OCR as
        # a fallback so plate numbers inside document images are searchable.
        if len(re.sub(r"\s+", "", text)) >= 8:
            return text
        ocr_text = extract_text_with_ocr(path)
        return text + "\n" + ocr_text
    except Exception as e:
        log.warning("PDF read error %s: %s", path.name, e)
        return ""


def extract_text_with_ocr(path: Path) -> str:
    """Render image-only PDF pages and OCR them with Tesseract.

    Python packages are optional; if Tesseract is not installed, the normal
    pypdf result is preserved and a clear warning is logged.
    """
    try:
        import fitz  # PyMuPDF
        import pytesseract
        from PIL import Image, ImageOps
    except ImportError:
        log.warning("OCR unavailable: install pymupdf, pytesseract, and pillow")
        return ""

    tess = os.environ.get("TESSERACT_CMD") or shutil.which("tesseract")
    if not tess:
        for candidate in (
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        ):
            if Path(candidate).exists():
                tess = candidate
                break
    if not tess:
        log.warning(
            "OCR unavailable: install Tesseract OCR and set TESSERACT_CMD if needed"
        )
        return ""
    pytesseract.pytesseract.tesseract_cmd = tess

    try:
        doc = fitz.open(str(path))
        parts = []
        for page in doc:
            pix = page.get_pixmap(dpi=220, alpha=False)
            image = Image.open(io.BytesIO(pix.tobytes("png")))
            images = [image]
            # Viewer screenshots are usually landscape: WhatsApp's chat panel
            # is on the right, while the left side is the chat list/UI chrome.
            if image.width / max(image.height, 1) > 1.3:
                images.append(image.crop((int(image.width * 0.42), 0,
                                          image.width, image.height)))
            for candidate in images:
                prepared = ImageOps.grayscale(candidate)
                prepared = prepared.resize(
                    (prepared.width * 2, prepared.height * 2)
                )
                prepared = ImageOps.autocontrast(prepared)
                for config in ("--psm 11", "--psm 6", "--psm 3"):
                    parts.append(pytesseract.image_to_string(
                        prepared, config=config
                    ))
        doc.close()
        return "\n".join(parts)
    except Exception as e:
        log.warning("OCR failed for %s: %s", path.name, e)
        return ""


def find_plates_and_vins(text: str):
    return sorted(set(PLATE_RE.findall(text))), sorted(set(VIN_RE.findall(text)))


def run_extraction_pipeline():
    """Process every un-extracted PDF. Returns (count, summary lines)."""
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    pending = db.get_unextracted()
    lines = []
    processed = 0
    for row in pending:
        rel = Path(row["local_file_path"])
        path = rel if rel.is_absolute() else BASE_DIR / rel
        if not path.exists():
            lines.append(f"MISSING: {row['pdf_filename']}")
            continue
        text = extract_text_from_pdf(path)
        plates, vins = find_plates_and_vins(text)
        # A PDF can contain ordinary text plus a scanned plate image. OCR once
        # more when normal extraction found no identifiers.
        if not plates and not vins:
            ocr_text = extract_text_with_ocr(path)
            if ocr_text:
                plates, vins = find_plates_and_vins(text + "\n" + ocr_text)
        db.mark_extracted(row["pdf_filename"], plates, vins)
        processed += 1
        if plates:
            for p in plates:
                lines.append(
                    f"Extracted plate {p} from {row['pdf_filename']}"
                )
        else:
            lines.append(f"{row['pdf_filename']}: no plates found")
    lines.append(f"Saved to database ({processed} PDF(s) processed).")
    return processed, lines

# ---------------------------------------------------------------------------
# Background runner (so long scraping/extraction won't block the UI)
# ---------------------------------------------------------------------------
class BackgroundJob:
    """Tiny helper to track a running background job + collect logs."""
    def __init__(self):
        self.lock = threading.Lock()
        self.running = False
        self.kind = ""
        self.log: list[str] = []
        self.done = False
        self.error: Optional[str] = None

    def start(self, kind: str, fn):
        with self.lock:
            if self.running:
                return False
            self.running = True
            self.kind = kind
            self.log = []
            self.done = False
            self.error = None
        threading.Thread(target=self._run, args=(fn,), daemon=True).start()
        return True

    def _run(self, fn):
        try:
            fn(self)
        except Exception as e:
            self.error = f"{e}\n{traceback.format_exc()}"
        finally:
            with self.lock:
                self.running = False
                self.done = True

    def status(self):
        with self.lock:
            return {
                "running": self.running,
                "kind": self.kind,
                "log": list(self.log[-200:]),
                "done": self.done,
                "error": self.error,
            }

    def add(self, line: str):
        with self.lock:
            self.log.append(line)

job = BackgroundJob()

# ---------------------------------------------------------------------------
# WhatsApp scraper (Playwright)
# ---------------------------------------------------------------------------
class WhatsAppScraper:
    URL = "https://web.whatsapp.com/"

    def __init__(self, headless: bool = False, sink: Optional[BackgroundJob] = None):
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        SESSION_DIR.mkdir(parents=True, exist_ok=True)
        self.ctx = self._pw.chromium.launch_persistent_context(
            user_data_dir=str(SESSION_DIR),
            headless=headless,
            viewport={"width": 1280, "height": 800},
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-extensions",          # silence MetaMask/uBlock noise
            ],
        )
        self.page = (self.ctx.pages[0] if self.ctx.pages
                     else self.ctx.new_page())
        self.sink = sink

    def _log(self, msg: str):
        log.info(msg)
        if self.sink:
            self.sink.add(msg)

    def close(self):
        try:
            self.ctx.close()
        finally:
            self._pw.stop()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def open(self, timeout=180) -> bool:
        self._log("Opening WhatsApp Web ...")
        self.page.goto(self.URL, wait_until="domcontentloaded", timeout=60_000)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.page.locator(
                'div[data-testid="chat-list"], #side'
            ).count() > 0:
                self._log("Login detected.")
                return True
            time.sleep(2)
        self._log("Login timed out waiting for QR scan.")
        return False

    # Multiple selector candidates for WhatsApp Web's search box.
    # WhatsApp rotates these frequently, so we try each in order.
    SEARCH_BOX_SELECTORS = [
        'div[contenteditable="true"][data-testid="search-input-placeholder"]',
        'div[contenteditable="true"][aria-label="Search input textbox"]',
        'div[contenteditable="true"][aria-label*="Search"]',
        'div[contenteditable="true"][role="textbox"][data-testid="search-keyword-field"]',
        'div[data-testid="search"] input',
        '#side input[title="Search"]',
    ]
    # Candidates for the first result row in the search panel.
    SEARCH_RESULT_SELECTORS = [
        '#side div[data-testid="cell-frame-container"]',
        '#side [data-testid="chat-list-search-results"] [role="listitem"]',
        '#side [role="listitem"]',
    ]

    def _find_first_visible(self, selectors, timeout_ms=15_000):
        """Try each selector until one becomes visible; return its locator."""
        deadline = time.time() + (timeout_ms / 1000)
        last_err = None
        while time.time() < deadline:
            for sel in selectors:
                loc = self.page.locator(sel).first
                try:
                    if loc.count() and loc.is_visible():
                        return loc
                except Exception as e:
                    last_err = e
            time.sleep(0.4)
        return None

    def _human_type(self, locator, text, delay=0.08):
        """Click a contenteditable locator and type char-by-char to imitate human."""
        try:
            locator.click()
            # Clear any pre-existing text
            self.page.keyboard.press("Control+A")
            self.page.keyboard.press("Delete")
            for ch in text:
                self.page.keyboard.type(ch)
                time.sleep(delay)
        except Exception:
            locator.fill(text)  # fallback to instant fill

    def search_and_open(self, target: str) -> bool:
        self._log(f"Looking up chat: {target}")
        phone = re.sub(r"\D", "", target)
        if 8 <= len(phone) <= 15:
            return self.open_chat_by_phone(phone)
        try:
            search = self._find_first_visible(self.SEARCH_BOX_SELECTORS, 15_000)
            if search is None:
                self._log("  Could not find the search box (selectors all failed).")
                return False
            # WhatsApp search is usually a contenteditable div; either type or fill.
            self._human_type(search, target)
            time.sleep(1.8)   # let results appear + debounce
            # Prefer an exact result title. This prevents clicking unrelated chat rows.
            clean_title = target.replace('"', "")
            exact = self.page.locator(
                f'#side span[title="{clean_title}"], '
                f'#side div[title="{clean_title}"]'
            ).first
            try:
                first = exact if exact.count() and exact.is_visible() else None
            except Exception:
                first = None
            if first is None:
                first = self._find_first_visible(self.SEARCH_RESULT_SELECTORS, 12_000)
            if first is None:
                self._log("  No search result appeared. Use Manual contact selection instead.")
                # Clear search and bail out
                try:
                    self.page.keyboard.press("Escape")
                except Exception:
                    pass
                return False
            first.click()
            time.sleep(2.5)   # let the conversation panel render
            # Exit search so the chat list state is clean for the next chat.
            try:
                self.page.keyboard.press("Escape")
                time.sleep(0.3)
                self.page.keyboard.press("Escape")
            except Exception:
                pass
            return True
        except Exception as e:
            self._log(f"Could not open chat '{target}': {e}")
            return False

    def open_chat_by_phone(self, phone: str) -> bool:
        """Open a known international number without using the chat search UI."""
        self._log(f"Opening WhatsApp chat directly for +{phone} ...")
        try:
            self.page.goto(
                f"https://web.whatsapp.com/send?phone={phone}",
                wait_until="domcontentloaded", timeout=60_000,
            )
            # WhatsApp sometimes presents a confirmation button before chat opens.
            for label in ("Continue to Chat", "Continue", "Open chat"):
                button = self.page.get_by_role("button", name=label).first
                try:
                    if button.count() and button.is_visible():
                        button.click()
                        break
                except Exception:
                    continue
            deadline = time.time() + 20
            while time.time() < deadline:
                # WhatsApp may render a conversation with a changed message-list
                # selector. The header/composer are also reliable indicators.
                chat_open = (
                    self._find_message_list() is not None
                    or self.page.locator('#main header').count() > 0
                    or self.page.locator(
                        '#main [contenteditable="true"][role="textbox"]'
                    ).count() > 0
                )
                if chat_open:
                    self._log(f"Opened chat for +{phone}.")
                    return True
                body_text = (self.page.locator('body').inner_text() or '').lower()
                if 'phone number shared via url is invalid' in body_text:
                    self._log(
                        f"WhatsApp rejected +{phone} as an invalid WhatsApp number."
                    )
                    return False
                time.sleep(1)
            self._log(f"Current WhatsApp URL: {self.page.url}")
            self._log("No conversation header/message composer appeared after navigation.")
            self._log(
                f"Could not open +{phone}. Confirm the number is in international format "
                "and is registered on WhatsApp."
            )
            return False
        except Exception as e:
            self._log(f"Could not navigate to +{phone}: {e}")
            return False

    def get_chat_info(self):
        info = {"name": "", "phone": ""}
        try:
            header = self.page.locator(
                'header[data-testid="conversation-info-header"], #main header'
            ).first
            for selector in ('span[title]', '[data-testid="conversation-info-header-chat-title"]'):
                name_el = header.locator(selector).first
                try:
                    if name_el.count():
                        info["name"] = (
                            name_el.get_attribute("title") or name_el.inner_text()
                        ).strip()
                        if info["name"]:
                            break
                except Exception:
                    continue
            header_text = header.inner_text(timeout=2_000) or ""
            phone_match = re.search(r"\+?\d[\d ()-]{7,}\d", header_text)
            if phone_match:
                info["phone"] = phone_match.group(0).strip()
            if not info["name"]:
                # Current WhatsApp headers often expose only visible text.
                lines = [line.strip() for line in header_text.splitlines() if line.strip()]
                for line in lines:
                    if not re.search(r"\d", line) and line.lower() not in {
                        "online", "last seen", "typing..."
                    }:
                        info["name"] = line
                        break
        except Exception:
            pass
        return info

    def _find_message_list(self):
        for sel in MESSAGE_LIST_SELECTORS:
            loc = self.page.locator(sel).first
            try:
                if loc.wait_for(state="visible", timeout=8_000):
                    return loc
            except Exception:
                continue
        return None

    def scroll_back(self, target_date: datetime, max_scrolls=200):
        panel = self._find_message_list()
        if panel is None:
            self._log("Message list not found - skipping scroll.")
            return
        self._log("Scrolling back to load older messages ...")
        last = -1
        stall = 0
        for i in range(max_scrolls):
            seps = self.page.locator('div[data-testid="date-separator"]')
            if seps.count():
                txt = (seps.first.inner_text(timeout=2_000) or "").strip()
                try:
                    seen = datetime.strptime(txt, "%B %d, %Y")
                    if seen <= target_date:
                        self._log(
                            f"Reached {seen.date()} after {i} scrolls."
                        )
                        return
                except ValueError:
                    pass
            try:
                current = self.page.evaluate(
                    "(el)=>el.scrollTop", panel.element_handle()
                )
            except Exception:
                current = 0
            if current == last:
                stall += 1
                if stall > 5:
                    self._log("Scroll stalled - reached top.")
                    return
            else:
                stall = 0
            last = current
            try:
                self.page.evaluate(
                    "(el)=>{el.scrollTop = el.scrollTop - el.clientHeight*0.9;}",
                    panel.element_handle(),
                )
            except Exception:
                pass
            time.sleep(random.uniform(1.0, 1.8))
        self._log("Reached max scroll count.")

    def collect_pdfs(self):
        """Return unique visible PDF document cards from the loaded history."""
        results = []
        seen = set()
        card_selectors = [
            'div[data-testid="media-document"]',
            'div[data-testid="document"]',
            'a[href*=".pdf"]',
            # Restrict text matching to message containers. A broad
            # div:has-text selector returns large parent panels that are not
            # clickable document cards.
            'div[data-testid="msg-container"]',
            'div.message-in',
            'div.message-out',
        ]
        for selector in card_selectors:
            cards = self.page.locator(selector)
            self._log(f"PDF selector {selector!r}: {cards.count()} candidate(s).")
            for i in range(cards.count()):
                try:
                    card = cards.nth(i)
                    txt = card.inner_text(timeout=1_000)
                    match = re.search(r"[^\\/\n]+\.pdf\b", txt, re.IGNORECASE)
                    if not match:
                        continue
                    name = match.group(0).strip()
                    # Prefer the smallest nested element containing the PDF
                    # name. Clicking the full message container often only
                    # selects the message instead of opening the document.
                    clickable = card
                    nested = card.get_by_text(re.compile(r"\.pdf$", re.IGNORECASE)).first
                    try:
                        if nested.count() and nested.is_visible():
                            clickable = nested
                    except Exception:
                        pass
                    identity = name.lower()
                    if identity in seen:
                        continue
                    seen.add(identity)
                    results.append({"filename": name, "locator": clickable, "message": card})
                except Exception:
                    continue
        return results

    def _download_from_viewer_page(self, viewer_page, target_dir: Path,
                                   suggested_name="document.pdf") -> Optional[Path]:
        """Download from a WhatsApp PDF viewer page or embedded frame."""
        try:
            roots = [viewer_page]
            iframe = viewer_page.locator('iframe[data-testid="pdf-viewer-iframe"]').first
            if iframe.count() and iframe.is_visible() and iframe.content_frame:
                roots.append(iframe.content_frame)
            selectors = [
                'button[aria-label*="download" i]',
                'a[download]',
                '[data-testid="download"]',
                '[data-icon="download"]',
                'button:has-text("Download")',
            ]
            for root in roots:
                for selector in selectors:
                    candidate = root.locator(selector).first
                    try:
                        if not candidate.count() or not candidate.is_visible():
                            continue
                        with viewer_page.expect_download(timeout=60_000) as dl_info:
                            candidate.click(force=True)
                        download = dl_info.value
                        safe = re.sub(r"[^\w\-.]", "_", suggested_name)
                        if not safe.lower().endswith(".pdf"):
                            safe += ".pdf"
                        dest = target_dir / safe
                        download.save_as(str(dest))
                        self._log(f"Downloading PDF from viewer... -> {dest.name}")
                        return dest
                    except Exception:
                        continue
            try:
                buttons = viewer_page.locator('button').all_inner_texts()
                self._log(
                    f"Viewer diagnostic: url={viewer_page.url}, "
                    f"buttons={buttons[:10]}"
                )
            except Exception:
                pass
        except Exception:
            return None
        return None

    def _download_from_open_viewer(self, target_dir: Path,
                                   suggested_name="document.pdf") -> Optional[Path]:
        """Check the WhatsApp page and any PDF viewer popup/page."""
        for viewer_page in list(self.ctx.pages):
            result = self._download_from_viewer_page(
                viewer_page, target_dir, suggested_name
            )
            if result:
                return result
        return None

    def _capture_open_document(self, target_dir: Path, suggested_name: str) -> Optional[Path]:
        """Last-resort fallback: capture the visible PDF and OCR the capture.

        WhatsApp sometimes renders its PDF viewer as a protected canvas with no
        download DOM at all. The visible page is still useful for plate search.
        The generated PDF contains the rendered page and is processed by OCR.
        """
        try:
            pages = list(self.ctx.pages)
            viewer_page = pages[-1] if pages else self.page
            image_bytes = viewer_page.screenshot(full_page=False)
            from PIL import Image
            image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
            safe = re.sub(r"[^\w\-.]", "_", suggested_name or "document.pdf")
            stem = Path(safe).stem or "document"
            dest = target_dir / f"{stem}_viewer_capture.pdf"
            image.save(str(dest), "PDF", resolution=150.0)
            self._log(
                f"No viewer download API found; captured visible PDF page for OCR -> {dest.name}"
            )
            return dest
        except Exception as e:
            self._log(f"Viewer screenshot/OCR fallback failed: {e}")
            return None

    def download_pdf(self, card_locator, target_dir: Path, suggested_name="document.pdf") -> Optional[Path]:
        target_dir.mkdir(parents=True, exist_ok=True)
        try:
            # The previous attachment's viewer can remain open. Close it before
            # clicking the next card, otherwise every loop downloads the same
            # currently-open PDF.
            try:
                existing_viewer = self.page.locator(
                    'iframe[data-testid="pdf-viewer-iframe"]'
                ).first
                if existing_viewer.count() and existing_viewer.is_visible():
                    self.page.keyboard.press("Escape")
                    time.sleep(0.7)
            except Exception:
                pass
            # Document cards may either start a download immediately or open a
            # preview with a dedicated Download button. Support both behaviours.
            viewer = self.page.locator('iframe[data-testid="pdf-viewer-iframe"]').first
            if viewer.count() and viewer.is_visible():
                self._log("PDF preview iframe is open; using its download control.")
                result = self._download_from_open_viewer(target_dir, suggested_name)
                if result:
                    return result
                self._log("PDF iframe found, but its Download control was not visible.")
            try:
                with self.page.expect_download(timeout=8_000) as dl_info:
                    # JS click avoids an iframe overlay intercepting Playwright's
                    # normal pointer click on the filename span.
                    card_locator.evaluate("(el) => el.click()")
                download = dl_info.value
            except Exception:
                # The first click already happened. It opened the preview but
                # produced no browser download event. Do not click again: a
                # second click closes the preview and captures the chat screen.
                time.sleep(2)
                result = self._download_from_open_viewer(target_dir, suggested_name)
                if result:
                    return result
                self._log(
                    f"Checked {len(self.ctx.pages)} browser page(s), but no PDF download control was found."
                )
                captured = self._capture_open_document(target_dir, suggested_name)
                if captured:
                    return captured
                download_button = self._find_first_visible([
                    'button[aria-label="Download"]',
                    'button[aria-label*="download" i]',
                    'button[data-testid="download"]',
                    '[data-icon="download"]',
                ], 8_000)
                if download_button is None:
                    self._log("Document opened but WhatsApp's download button was not found.")
                    return None
                with self.page.expect_download(timeout=60_000) as dl_info:
                    download_button.click()
                download = dl_info.value
                safe = re.sub(r"[^\w\-.]", "_", suggested_name)
            if not safe.lower().endswith(".pdf"):
                safe += ".pdf"
            dest = target_dir / safe
            download.save_as(str(dest))
            self._log(f"Downloading PDF... -> {dest.name}")
            return dest
        except Exception as e:
            self._log(f"Download failed: {e}")
            return None

    def scrape_chat(self, target, history_days=DEFAULT_HISTORY_DAYS) -> int:
        if not self.search_and_open(target):
            return 0
        return self.scrape_open_chat(history_days, target)

    def scrape_open_chat(self, history_days=DEFAULT_HISTORY_DAYS, label="selected chat") -> int:
        """Scrape the conversation currently visible in the WhatsApp window.

        This is the reliable fallback when WhatsApp changes its chat-search DOM:
        the user simply clicks their desired contact in the visible browser.
        """
        info = self.get_chat_info()
        if not info["phone"]:
            possible_phone = re.sub(r"\D", "", label)
            if 8 <= len(possible_phone) <= 15:
                info["phone"] = f"+{possible_phone}"
        if not info["name"]:
            info["name"] = label
        if not info["name"] and self._find_message_list() is None:
            # Some chats expose no title until the header finishes rendering.
            try:
                header = self.page.locator('#main header').first
                if header.count() and header.is_visible():
                    info["name"] = "Selected WhatsApp chat"
            except Exception:
                pass
        if not info["name"] and self._find_message_list() is None:
            self._log("No open chat detected. Select a contact, then try again.")
            return 0
        self._log(
            f"Chat opened: name={info['name']!r} phone={info['phone']!r}"
        )
        target_date = datetime.now() - timedelta(days=history_days)
        self.scroll_back(target_date)
        cards = self.collect_pdfs()
        self._log(f"Found {len(cards)} PDF attachment card(s).")
        saved = 0
        used_names = set()
        for index, c in enumerate(cards, start=1):
            original_name = c["filename"] or f"document_{index}.pdf"
            name_path = Path(original_name)
            stem = name_path.stem or f"document_{index}"
            suffix = name_path.suffix or ".pdf"
            unique_name = f"{stem}{suffix}"
            counter = 2
            while unique_name.lower() in used_names or (PDF_DIR / unique_name).exists():
                unique_name = f"{stem}_{counter}{suffix}"
                counter += 1
            used_names.add(unique_name.lower())
            dest = self.download_pdf(c["locator"], PDF_DIR, unique_name)
            if dest is None:
                continue
            # WhatsApp embeds the original message timestamp in this
            # attribute. Fall back to the scrape time when unavailable.
            date_str = datetime.now().isoformat(timespec="seconds")
            try:
                stamp = c["message"].get_attribute("data-pre-plain-text")
                if stamp:
                    parsed = re.search(
                        r"\[(\d{1,2}[:.]\d{2})[, ]+(\d{1,2})[/.](\d{1,2})[/.](\d{2,4})",
                        stamp,
                    )
                    if parsed:
                        date_str = stamp
            except Exception:
                pass
            db.upsert_pdf(
                customer_name=info["name"],
                phone_number=info["phone"],
                message_date=date_str,
                pdf_filename=dest.name,
                local_file_path=str(dest),
                file_hash=file_hash(dest),
            )
            self._log(
                f"Saved {dest.name} | customer={info['name']} | phone={info['phone'] or '-'}"
            )
            saved += 1
        self._log(f"Saved {saved} PDF(s) to database for {label}.")
        return saved

# ---------------------------------------------------------------------------
# Flask app + UI
# ---------------------------------------------------------------------------
app = Flask(__name__)

BASE_HTML = r"""
<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WhatsApp Plate Finder</title>
<style>
  :root{
    --bg:#0f172a; --panel:#1e293b; --panel2:#334155;
    --text:#e2e8f0; --muted:#94a3b8; --accent:#22c55e;
    --accent2:#3b82f6; --danger:#ef4444; --warn:#f59e0b;
  }
  *{box-sizing:border-box}
  body{margin:0;font-family:-apple-system,BlinkMacSystemFont,
       'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
       background:var(--bg);color:var(--text)}
  header{background:#0b1220;padding:14px 22px;display:flex;
         align-items:center;justify-content:space-between;
         border-bottom:1px solid #1e293b}
  header h1{font-size:18px;margin:0;color:#fff}
  header .sub{color:var(--muted);font-size:12px}
  nav{display:flex;gap:6px;padding:10px 22px;background:var(--panel);
      border-bottom:1px solid #334155;flex-wrap:wrap}
  nav a{color:var(--muted);text-decoration:none;padding:8px 14px;
        border-radius:6px;font-size:14px;font-weight:500}
  nav a:hover{background:var(--panel2);color:#fff}
  nav a.active{background:var(--accent2);color:#fff}
  main{padding:22px;max-width:1100px;margin:0 auto}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
        gap:14px;margin-bottom:24px}
  .card{background:var(--panel);border:1px solid #334155;
        border-radius:10px;padding:18px}
  .card .k{color:var(--muted);font-size:12px;text-transform:uppercase;
           letter-spacing:.05em}
  .card .v{font-size:26px;font-weight:600;margin-top:4px}
  .panel{background:var(--panel);border:1px solid #334155;
         border-radius:10px;padding:20px;margin-bottom:20px}
  .panel h2{margin:0 0 14px;font-size:16px}
  input,select,button,textarea{
    font-family:inherit;font-size:14px;padding:9px 12px;
    border-radius:6px;border:1px solid #334155;background:#0b1220;
    color:var(--text);outline:none}
  input:focus,select:focus,textarea:focus{border-color:var(--accent2)}
  button{cursor:pointer;background:var(--accent2);border:none;
         color:#fff;font-weight:500;padding:10px 18px;transition:.15s}
  button:hover{filter:brightness(1.1)}
  button.ghost{background:transparent;border:1px solid #334155;color:var(--text)}
  button.danger{background:var(--danger)}
  button.ok{background:var(--accent)}
  .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{text-align:left;padding:10px 12px;border-bottom:1px solid #334155}
  th{color:var(--muted);font-weight:500;font-size:12px;
     text-transform:uppercase;letter-spacing:.05em}
  tr:hover{background:#1e293b80}
  .tag{display:inline-block;background:#334155;color:#a5f3fc;
       padding:2px 8px;border-radius:999px;font-size:11px;margin:1px}
  .log{background:#0b1220;border:1px solid #334155;
       border-radius:8px;padding:12px;max-height:340px;overflow:auto;
       font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;
       color:#cbd5e1;white-space:pre-wrap}
  .badge{display:inline-block;padding:3px 10px;border-radius:999px;
         font-size:11px;font-weight:600}
  .badge.run{background:var(--warn);color:#1b1300}
  .badge.done{background:var(--accent);color:#042b14}
  .badge.err{background:var(--danger);color:#fff}
  .badge.idle{background:#334155;color:#cbd5e1}
  .errbox{background:#3b0d0d;border:1px solid #7f1d1d;
          color:#fecaca;padding:12px;border-radius:8px;
          font-family:ui-monospace,Menlo,Consolas,monospace;
          font-size:12px;white-space:pre-wrap;margin-top:12px}
  .muted{color:var(--muted);font-size:12px}
  a.dl{color:var(--accent2);text-decoration:none}
  a.dl:hover{text-decoration:underline}
  .empty{color:var(--muted);padding:30px;text-align:center;
         font-style:italic}
</style></head>
<body>
<header>
  <div>
    <h1>WhatsApp Plate Finder</h1>
    <div class="sub">PDF scrape &middot; plate extraction &middot; local search</div>
  </div>
  <div id="job-status" class="badge idle">idle</div>
</header>
<nav>
  <a href="/" data-tab="dashboard">Dashboard</a>
  <a href="/scrape" data-tab="scrape">Scrape chats</a>
  <a href="/extract" data-tab="extract">Run extraction</a>
  <a href="/search" data-tab="search">Search</a>
  <a href="/index" data-tab="index">Full index</a>
  <a href="/sync" data-tab="sync">Incremental sync</a>
</nav>
<main>
  __PAGE_CONTENT__
</main>
<script>
let jobState = {running:false, kind:"", log:[], done:false, error:null};
async function refreshStatus(){
  try{
    const r = await fetch('/api/job/status');
    const d = await r.json();
    jobState = d;
    const el = document.getElementById('job-status');
    if(d.running){
      el.textContent = 'running: '+(d.kind||'job');
      el.className = 'badge run';
    } else if(d.error){
      el.textContent = 'last error';
      el.className = 'badge err';
    } else if(d.done){
      el.textContent = 'last job done';
      el.className = 'badge done';
    } else {
      el.textContent = 'idle';
      el.className = 'badge idle';
    }
    const box = document.getElementById('live-log');
    if(box){
      box.textContent = (d.log||[]).join('\\n');
      box.scrollTop = box.scrollHeight;
    }
    const errbox = document.getElementById('live-err');
    if(errbox){
      errbox.style.display = d.error ? 'block' : 'none';
      errbox.textContent = d.error || '';
    }
  }catch(e){}
  if(jobState.running || document.getElementById('autorefresh')){
    setTimeout(refreshStatus, 1500);
  } else {
    setTimeout(refreshStatus, 5000);
  }
}
async function startJob(event, form){
  event.preventDefault();
  const response = await fetch(form.action, {method:'POST', body:new FormData(form)});
  if(!response.ok){
    alert(await response.text());
    return false;
  }
  refreshStatus();
  return false;
}
async function resetDatabase(){
  if(!confirm('Reset the database and delete downloaded PDFs?')) return;
  const response = await fetch('/api/reset', {method:'POST'});
  if(!response.ok){ alert(await response.text()); return; }
  alert('Database and downloaded PDFs reset. WhatsApp login was preserved.');
  window.location.reload();
}
refreshStatus();
</script>
</body></html>
"""

DASHBOARD_HTML = BASE_HTML + r"""
<div class="grid">
  <div class="card"><div class="k">PDFs indexed</div>
    <div class="v">{{ stats.total }}</div></div>
  <div class="card"><div class="k">Extracted</div>
    <div class="v">{{ stats.extracted }}</div></div>
  <div class="card"><div class="k">With plates</div>
    <div class="v">{{ stats.with_plates }}</div></div>
</div>

<div class="panel">
  <h2>Quick actions</h2>
  <div class="row">
    <a href="/scrape"><button>Scrape chats</button></a>
    <a href="/extract"><button class="ghost">Run extraction</button></a>
    <a href="/search"><button class="ghost">Search</button></a>
    <a href="/sync"><button class="ghost">Incremental sync</button></a>
    <button class="danger" onclick="resetDatabase()">Reset database</button>
  </div>
</div>

<div class="panel">
  <h2>Background log</h2>
  <div id="live-log" class="log">idle</div>
  <div id="live-err" class="errbox" style="display:none"></div>
</div>
"""

SCRAPE_HTML = BASE_HTML + r"""
<div class="panel">
  <h2>Scrape customer chats</h2>
  <p class="muted">Recommended: use Manual contact selection. Chromium opens,
  then click the exact contact you want in WhatsApp before the countdown ends.
  This avoids WhatsApp's frequently changing search interface.</p>
  <form method="post" action="/api/scrape" onsubmit="return startJob(event,this)">
    <div class="row">
      <input name="targets" placeholder="Optional: exact contact name or phone"
             style="flex:1;min-width:280px">
      <label class="muted">days back:
        <input type="number" name="days" value="365"
               style="width:80px"></label>
      <label class="muted"><input type="checkbox" name="manual_select" checked>
        I will select the contact</label>
      <label class="muted">wait seconds:
        <input type="number" name="wait_seconds" value="30" min="10" max="300"
               style="width:70px"></label>
      <button class="ok">Start scrape</button>
    </div>
  </form>
</div>

<div class="panel">
  <h2>Live log</h2>
  <div id="live-log" class="log">waiting to start ...</div>
  <div id="live-err" class="errbox" style="display:none"></div>
</div>
<div class="muted" id="autorefresh"></div>
"""

EXTRACT_HTML = BASE_HTML + r"""
<div class="panel">
  <h2>Run PDF extraction pipeline</h2>
  <p class="muted">Scans every un-extracted PDF in <code>./downloaded_pdfs</code>,
  extracts plate numbers (6-7 digit) and VINs (17-char), updates the database.</p>
  <form method="post" action="/api/extract">
    <button class="ok">Run extraction now</button>
  </form>
</div>
<div class="panel">
  <h2>Live log</h2>
  <div id="live-log" class="log">idle</div>
  <div id="live-err" class="errbox" style="display:none"></div>
</div>
<div class="muted" id="autorefresh"></div>
"""

SEARCH_HTML = BASE_HTML + r"""
<div class="panel">
  <h2>Search</h2>
  <p class="muted">Enter a plate, a date, or both. When both are entered,
  results must match both values. The plate search uses extracted PDF text.</p>
  <form method="get" action="/search">
    <div class="row">
      <input name="plate" value="{{ plate }}" placeholder="Plate number, e.g. 1943628"
             style="flex:1;min-width:220px">
      <input type="date" name="date" value="{{ date }}" style="min-width:170px">
      <button>Search</button>
    </div>
  </form>
</div>

{% if results is not none %}
<div class="panel">
  <h2>Results ({{ results|length }})</h2>
  {% if results|length == 0 %}
    <div class="empty">No matching records.</div>
  {% else %}
  <table>
    <thead><tr>
      <th>Customer</th><th>Phone</th><th>Date sent</th>
      <th>PDF</th><th>Plates</th><th>VINs</th><th>Download</th>
    </tr></thead>
    <tbody>
    {% for r in results %}
      <tr>
        <td>{{ r.customer_name or '-' }}</td>
        <td>{{ r.phone_number or '-' }}</td>
        <td>{{ r.message_date or '-' }}</td>
        <td>{{ r.pdf_filename }}</td>
        <td>
          {% for p in (r.plates|from_json if r.plates else []) %}
            <span class="tag">{{ p }}</span>
          {% endfor %}
        </td>
        <td>
          {% for v in (r.vins|from_json if r.vins else []) %}
            <span class="tag">{{ v }}</span>
          {% endfor %}
        </td>
        <td>
          {% if r.local_file_path %}
            <a class="dl" href="/api/pdf/{{ r.id }}">download</a>
          {% endif %}
        </td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
  {% endif %}
</div>
{% endif %}
"""

INDEX_HTML = BASE_HTML + r"""
<div class="panel">
  <h2>Full index ({{ rows|length }} records)</h2>
  {% if rows|length == 0 %}
    <div class="empty">Database is empty. Run a scrape first.</div>
  {% else %}
  <table>
    <thead><tr>
      <th>Customer</th><th>Phone</th><th>Date sent</th>
      <th>PDF</th><th>Plates</th><th>VINs</th><th>PDF</th>
    </tr></thead>
    <tbody>
    {% for r in rows %}
      <tr>
        <td>{{ r.customer_name or '-' }}</td>
        <td>{{ r.phone_number or '-' }}</td>
        <td>{{ r.message_date or '-' }}</td>
        <td>{{ r.pdf_filename }}</td>
        <td>
          {% for p in (r.plates|from_json if r.plates else []) %}
            <span class="tag">{{ p }}</span>
          {% endfor %}
        </td>
        <td>
          {% for v in (r.vins|from_json if r.vins else []) %}
            <span class="tag">{{ v }}</span>
          {% endfor %}
        </td>
        <td><a class="dl" href="/api/pdf/{{ r.id }}">download</a></td>
      </tr>
    {% endfor %}
    </tbody>
  </table>
  {% endif %}
</div>
"""

SYNC_HTML = BASE_HTML + r"""
<div class="panel">
  <h2>Incremental sync</h2>
  <p class="muted">Re-opens each listed chat and only looks at the most recent
  {{ sync_days }} days. Useful to pull new PDFs without re-scanning a full year.</p>
  <form method="post" action="/api/sync">
    <div class="row">
      <input name="targets" placeholder="comma-separated chat keys"
             style="flex:1;min-width:280px" required>
      <button class="ok">Start sync</button>
    </div>
  </form>
</div>
<div class="panel">
  <h2>Live log</h2>
  <div id="live-log" class="log">idle</div>
  <div id="live-err" class="errbox" style="display:none"></div>
</div>
<div class="muted" id="autorefresh"></div>
"""

# Jinja filter to parse JSON plate/vin lists
app.jinja_env.filters["from_json"] = lambda s: json.loads(s) if s else []


def render_page(content_template: str, **ctx) -> str:
    """Render `content_template` (a plain fragment, no base block) then
    splice it into BASE_HTML via a placeholder. This avoids Jinja template
    inheritance issues with render_template_string + child blocks."""
    # Page constants historically include BASE_HTML for readability. Strip it
    # before rendering so the browser receives exactly one document.
    fragment = content_template[len(BASE_HTML):] if content_template.startswith(BASE_HTML) else content_template
    body = render_template_string(fragment, **ctx)
    html = BASE_HTML.replace("__PAGE_CONTENT__", body)
    # Render any Jinja that was inside BASE itself (placeholder is gone).
    return html


@app.route("/")
def page_dashboard():
    return render_page(DASHBOARD_HTML, stats=db.stats())


@app.route("/scrape")
def page_scrape():
    return render_page(SCRAPE_HTML)


@app.route("/extract")
def page_extract():
    return render_page(EXTRACT_HTML)


@app.route("/search")
def page_search():
    plate = re.sub(r"\s+", "", (request.args.get("plate") or "").strip())
    date_str = (request.args.get("date") or "").strip()
    results = None
    if plate or date_str:
        results = db.search_by_plate_and_date(plate, date_str)
    rows = [dict(r) for r in (results or [])]
    return render_page(SEARCH_HTML, plate=plate, date=date_str, results=rows)


@app.route("/index")
def page_index():
    rows = [dict(r) for r in db.get_all()]
    return render_page(INDEX_HTML, rows=rows)


@app.route("/sync", methods=["GET"])
def page_sync():
    return render_page(SYNC_HTML, sync_days=INCREMENTAL_SYNC_DAYS)


# --- API --------------------------------------------------------------------
@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    targets_raw = (request.form.get("targets") or "").strip()
    # Manual mode is deliberately visible: the user chooses the actual contact
    # in WhatsApp, avoiding fragile DOM automation for the chat search field.
    manual_select = request.form.get("manual_select") == "on"
    days = max(1, int(request.form.get("days") or DEFAULT_HISTORY_DAYS))
    wait_seconds = max(10, min(300, int(request.form.get("wait_seconds") or 30)))
    targets = [t.strip() for t in targets_raw.split(",") if t.strip()]
    if not manual_select and not targets:
        return "Enter at least one contact name/phone, or select manual mode.", 400

    def work(s):
        scraper = WhatsAppScraper(headless=False, sink=s)
        with scraper:
            if not scraper.open():
                s.add("Login required - aborted.")
                return
            total = 0
            if manual_select:
                s.add(
                    f"Select the contact in the visible WhatsApp window now. "
                    f"Waiting {wait_seconds} seconds ..."
                )
                time.sleep(wait_seconds)
                total = scraper.scrape_open_chat(days, "manually selected chat")
                s.add(f"Total: saved {total} PDF(s) from the selected chat.")
            else:
                for t in targets:
                    total += scraper.scrape_chat(t, history_days=days)
                s.add(f"Total: saved {total} PDF(s) across {len(targets)} chat(s).")
        s.add("Scrape complete. Running extraction ...")
        n, lines = run_extraction_pipeline()
        for l in lines:
            s.add(l)

    if not job.start("scrape", work):
        return "A job is already running", 409
    return jsonify({"ok": True})


@app.route("/api/extract", methods=["POST"])
def api_extract():
    def work(s):
        lines_before = []
        n, lines = run_extraction_pipeline()
        for l in lines:
            s.add(l)

    if not job.start("extract", work):
        return "A job is already running", 409
    return jsonify({"ok": True})


@app.route("/api/sync", methods=["POST"])
def api_sync():
    targets_raw = (request.form.get("targets") or "").strip()
    targets = [t.strip() for t in targets_raw.split(",") if t.strip()]
    if not targets:
        return "No targets provided", 400

    def work(s):
        scraper = WhatsAppScraper(headless=False, sink=s)
        with scraper:
            if not scraper.open():
                s.add("Login required - aborted.")
                return
            total = 0
            for t in targets:
                total += scraper.scrape_chat(t, history_days=INCREMENTAL_SYNC_DAYS)
            s.add(f"Incremental sync saved {total} new PDF(s).")
        s.add("Running extraction ...")
        n, lines = run_extraction_pipeline()
        for l in lines:
            s.add(l)

    if not job.start("sync", work):
        return "A job is already running", 409
    return jsonify({"ok": True})


@app.route("/api/job/status")
def api_job_status():
    return jsonify(job.status())


@app.route("/api/reset", methods=["POST"])
def api_reset():
    if job.status()["running"]:
        return "A background job is running. Stop it before resetting.", 409
    db.reset()
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    for path in PDF_DIR.iterdir():
        if path.is_file():
            try:
                path.unlink()
            except OSError as e:
                log.warning("Could not delete %s: %s", path, e)
    return jsonify({"ok": True})


@app.route("/api/pdf/<int:pdf_id>")
def api_download_pdf(pdf_id):
    rows = db.conn.execute("SELECT * FROM pdfs WHERE id=?", (pdf_id,)).fetchall()
    if not rows:
        return "Not found", 404
    path = Path(rows[0]["local_file_path"])
    if not path.is_absolute():
        path = BASE_DIR / path
    if not path.exists():
        return "File missing on disk", 404
    return send_file(path, as_attachment=True,
                    download_name=rows[0]["pdf_filename"])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    print(f"\n  WhatsApp Plate Finder GUI")
    print(f"  Open in your browser:  http://{HOST}:{PORT}\n")
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
