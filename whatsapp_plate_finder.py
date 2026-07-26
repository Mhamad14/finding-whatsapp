"""
whatsapp_plate_finder.py
========================
Single-file application that:
  1. Automates WhatsApp Web login (persistent session -> scan QR only once).
  2. Scrapes customer chat histories and downloads PDF attachments.
  3. Extracts vehicle plate numbers / VINs from the PDFs.
  4. Indexes everything into a local SQLite database for fast offline search.

Dependencies:
    pip install playwright pypdf

After installing playwright you must also download the browser binaries once:
    python -m playwright install chromium

Usage:
    python whatsapp_plate_finder.py

Everything (config, session, downloads, DB) is stored under the current
working directory so the script remains fully portable.
"""

from __future__ import annotations

import os
import re
import sys
import time
import json
import random
import sqlite3
import hashlib
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

# --- Third-party -----------------------------------------------------------
# These are imported lazily inside the functions that need them so the
# search / query menu can still run even if Playwright is not installed.

# --- Configuration ---------------------------------------------------------
BASE_DIR        = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
SESSION_DIR     = BASE_DIR / "wa_session"           # persistent browser profile
PDF_DIR         = BASE_DIR / "downloaded_pdfs"      # downloaded PDF attachments
DB_PATH         = BASE_DIR / "whatsapp_index.db"    # SQLite index

# Toggle this for headless production runs.
HEADLESS        = False
# How far back to scroll in each chat.
DEFAULT_HISTORY_DAYS = 365
# Selector used by WhatsApp Web for the scrollable message list.
# WhatsApp changes these occasionally; we keep a few fallbacks.
MESSAGE_LIST_SELECTORS = [
    'div[aria-label="Message list"]',
    'div[role="panel"] div[data-testid="conversation-panel-messages"]',
    'div[id="main"] div.copyable-area',
]
# Selector candidates for a single message row.
MESSAGE_ROW_SELECTORS = [
    'div.message-in',
    'div.message-out',
    'div[data-testid="msg-container"]',
]

# Regex patterns we look for inside the PDF text.
#  - PLATE_RE: 6-7 digit numeric strings (example plate numbers).
#  - VIN_RE:   17-char VIN blocks (letters + digits, no I/O/Q per spec).
PLATE_RE = re.compile(r"\b\d{6,7}\b")
VIN_RE   = re.compile(r"\b[A-HJ-NPR-Z0-9]{17}\b")

# ----------------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("wa_plate")


# ============================================================================
# Database layer
# ============================================================================
class Database:
    """Thin wrapper around sqlite3 with helper methods."""

    def __init__(self, path: Path = DB_PATH) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path))
        self.conn.row_factory = sqlite3.Row
        self._create_schema()

    # -- schema -------------------------------------------------------------
    def _create_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pdfs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_name   TEXT,
                phone_number   TEXT,
                message_date   TEXT,
                pdf_filename    TEXT UNIQUE,
                local_file_path TEXT,
                file_hash       TEXT,
                plates          TEXT,          -- JSON list of plate numbers
                vins            TEXT,          -- JSON list of VIN numbers
                extracted       INTEGER DEFAULT 0,
                created_at      TEXT DEFAULT (datetime('now','localtime'))
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                chat_key        TEXT PRIMARY KEY,
                last_synced_at  TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_pdfs_date    ON pdfs(message_date);
            CREATE INDEX IF NOT EXISTS idx_pdfs_phone   ON pdfs(phone_number);
            CREATE INDEX IF NOT EXISTS idx_pdfs_customer ON pdfs(customer_name);
            """
        )
        self.conn.commit()

    # -- inserts -----------------------------------------------------------
    def upsert_pdf(self, customer_name: str,
                   phone_number: str,
                   message_date: str,
                   pdf_filename: str,
                   local_file_path: str,
                   file_hash: str) -> None:
        """Insert or ignore a PDF record (filename is unique)."""
        self.conn.execute(
            """
            INSERT OR IGNORE INTO pdfs
                (customer_name, phone_number, message_date, pdf_filename, local_file_path, file_hash)
            VALUES (?,?,?,?,?,?)
            """,
            (customer_name, phone_number, message_date, pdf_filename, local_file_path, file_hash),
        )
        self.conn.commit()

    def mark_extracted(self, pdf_filename: str, plates: list[str], vins: list[str]) -> None:
        self.conn.execute(
            "UPDATE pdfs SET plates=?, vins=?, extracted=1 WHERE pdf_filename=?",
            (json.dumps(plates), json.dumps(vins), pdf_filename),
        )
        self.conn.commit()

    def get_unextracted(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM pdfs WHERE extracted=0"
        ).fetchall()

    def get_all(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM pdfs ORDER BY message_date DESC").fetchall()

    # -- search ------------------------------------------------------------
    def search_by_plate(self, plate: str) -> list[sqlite3.Row]:
        rows = self.conn.execute(
            "SELECT * FROM pdfs WHERE plates LIKE ?",
            (f'%"{plate}"%',),
        ).fetchall()
        return rows

    def search_by_date(self, date_str: str) -> list[sqlite3.Row]:
        # Match dates stored as 'YYYY-MM-DD' (we normalise on insert).
        return self.conn.execute(
            "SELECT * FROM pdfs WHERE substr(message_date,1,10)=? ORDER BY message_date DESC",
            (date_str,),
        ).fetchall()

    def search_by_customer(self, name: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM pdfs WHERE customer_name LIKE ? ORDER BY message_date DESC",
            (f"%{name}%",),
        ).fetchall()

    # -- sync state ---------------------------------------------------------
    def set_sync_state(self, chat_key: str, when: Optional[str] = None) -> None:
        when = when or datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            "INSERT OR REPLACE INTO sync_state (chat_key, last_synced_at) VALUES (?,?)",
            (chat_key, when),
        )
        self.conn.commit()

    def get_sync_state(self, chat_key: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT last_synced_at FROM sync_state WHERE chat_key=?",
            (chat_key,),
        ).fetchone()
        return row["last_synced_at"] if row else None


# ============================================================================
# PDF extraction
# ============================================================================
def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_text_from_pdf(path: Path) -> str:
    """Return the concatenated text of every page, or '' on failure."""
    try:
        from pypdf import PdfReader
    except ImportError:
        log.warning("pypdf not installed - cannot read PDF text")
        return ""

    try:
        reader = PdfReader(str(path))
        parts: list[str] = []
        for page in reader.pages:
            try:
                parts.append(page.extract_text() or "")
            except Exception as e:  # per-page resilient
                log.debug("page error in %s: %s", path.name, e)
        return "\n".join(parts)
    except Exception as e:
        log.warning("Could not read PDF %s: %s", path.name, e)
        return ""


def find_plates_and_vins(text: str) -> tuple[list[str], list[str]]:
    plates = sorted(set(PLATE_RE.findall(text)))
    vins   = sorted(set(VIN_RE.findall(text)))
    return plates, vins


def run_pdf_extraction_pipeline(db: Database) -> int:
    """Walk ./downloaded_pdfs, parse every un-extracted PDF and update the DB.

    Returns the number of PDFs that were successfully processed.
    """
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    pending = db.get_unextracted()
    processed = 0
    for row in pending:
        rel_path = Path(row["local_file_path"])
        path = rel_path if rel_path.is_absolute() else BASE_DIR / rel_path
        print(f"  Parsing PDF: {row['pdf_filename']} ... ", end="", flush=True)
        if not path.exists():
            print("MISSING")
            log.warning("File not found on disk: %s", path)
            continue
        text = extract_text_from_pdf(path)
        plates, vins = find_plates_and_vins(text)
        db.mark_extracted(row["pdf_filename"], plates, vins)
        processed += 1
        if plates:
            print(f"found plates {plates}")
            for p in plates:
                print(f"    Extracted plate {p} from {row['pdf_filename']}")
        else:
            print("no plates")
    print(f"Saved to database ({processed} PDF(s) processed).")
    return processed


# ============================================================================
# Playwright / WhatsApp Web engine
# ============================================================================
class WhatsAppScraper:
    """Wraps a persistent Playwright context against WhatsApp Web."""

    URL = "https://web.whatsapp.com/"

    def __init__(self, headless: bool = HEADLESS) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            print("Playwright is not installed.  Run:\n  pip install playwright\n  python -m playwright install chromium")
            raise
        self._pw = sync_playwright().start()
        SESSION_DIR.mkdir(parents=True, exist_ok=True)
        self.ctx = self._pw.chromium.launch_persistent_context(
            user_data_dir=str(SESSION_DIR),
            headless=headless,
            viewport={"width": 1280, "height": 800},
            args=["--disable-blink-features=AutomationControlled"],
        )
        self.page = self.ctx.pages[0] if self.ctx.pages else self.ctx.new_page()
        self._logged_in = False

    # -- lifecycle ---------------------------------------------------------
    def close(self) -> None:
        try:
            self.ctx.close()
        finally:
            self._pw.stop()

    def __enter__(self) -> "WhatsAppScraper":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- navigation ---------------------------------------------------------
    def open(self) -> None:
        print("Opening WhatsApp Web ...")
        self.page.goto(self.URL, wait_until="domcontentloaded", timeout=60_000)
        self._wait_for_login()

    def _wait_for_login(self, timeout: int = 180) -> bool:
        """Block until QR login succeeded or timeout (seconds). expires."""
        print("Waiting for login (scan QR if needed) ...")
        deadline = time.time() + timeout
        while time.time() < deadline:
            # When logged in the search box or chat list is present.
            if self.page.locator('div[data-testid="chat-list"], #side').count() > 0:
                print("Login detected.")
                self._logged_in = True
                return True
            time.sleep(2)
        print("Login timed out.")
        return False

    # -- helpers ------------------------------------------------------------
    def _find_message_list(self):
        for sel in MESSAGE_LIST_SELECTORS:
            loc = self.page.locator(sel).first
            try:
                if loc.wait_for(state="visible", timeout=8_000):
                    return loc
            except Exception:
                continue
        return None

    def search_and_open_chat(self, target: str) -> bool:
        """Open a chat by phone number or contact name via the search bar."""
        print(f"Looking up chat: {target}")
        # Click the search input
        try:
            search = self.page.locator('div[data-testid="search"] input, #side input[title="Search"]').first
            search.wait_for(state="visible", timeout=15_000)
            search.click()
            search.fill(target)
            time.sleep(1.5)
            # First result
            first = self.page.locator('div[data-testid="cell-text-title-span-start"]').first
            first.wait_for(state="visible", timeout=10_000)
            first.click()
            time.sleep(2)
            return True
        except Exception as e:
            log.error("Could not open chat '%s': %s", target, e)
            return False

    def get_active_chat_info(self) -> dict:
        """Return {name, phone} for the chat currently displayed."""
        info = {"name": "", "phone": ""}
        try:
            header = self.page.locator('header[data-testid="conversation-info-header"]')
            name_el = header.locator('span[title]').first
            info["name"] = name_el.get_attribute("title") or ""
            # The subtitle often holds the phone number.
            sub = header.locator('span[dir="ltr"]').first
            info["phone"] = sub.inner_text(timeout=2_000).strip()
        except Exception:
            pass
        return info

    # -- scrolling ----------------------------------------------------------
    def scroll_back_until(self, target_date: datetime, max_scrolls: int = 200) -> None:
        """Scroll the message list upwards until we pass `target_date` or stall."""
        panel = self._find_message_list()
        if panel is None:
            print("  Cannot locate message list - skipping scroll.")
            return
        last_height = -1
        stall = 0
        for i in range(max_scrolls):
            # Find the earliest visible date separator.
            date_seps = self.page.locator('div[data-testid="date-separator"]')
            count = date_seps.count()
            if count:
                txt = (date_seps.first.inner_text(timeout=2_000) or "").strip()
                try:
                    seen = datetime.strptime(txt, "%B %d, %Y")
                    if seen <= target_date:
                        print(f"  Reached target date {seen.date()} after {i} scrolls.")
                        return
                except ValueError:
                    pass
            # Scroll up by evaluating on the panel.
            try:
                current = self.page.evaluate(
                    "(el) => el.scrollTop", panel.element_handle()
                )
            except Exception:
                current = 0
            if current == last_height:
                stall += 1
                if stall > 5:
                    print("  Scroll stalled - assuming we reached the top.")
                    return
            else:
                stall = 0
            last_height = current
            try:
                self.page.evaluate(
                    "(el)=>{ el.scrollTop = el.scrollTop - el.clientHeight*0.9; }",
                    panel.element_handle(),
                )
            except Exception:
                pass
            time.sleep(random.uniform(1.0, 1.8))   # mimic natural loading
        print("  Reached max scroll count.")

    # -- attachment collection ---------------------------------------------
    def collect_pdf_links(self) -> list[dict]:
        """Scan the visible message container for PDF attachments and return
        a list of dicts: {filename, date, download_locator}.  This is best
        effort - WhatsApp's DOM changes frequently.
        """
        results: list[dict] = []
        # PDF attachments are usually shown as a card with the .pdf filename.
        cards = self.page.locator('div[data-testid="media-document"], a[href*=".pdf"]')
        count = cards.count()
        for i in range(count):
            try:
                card = cards.nth(i)
                # Try to find the filename text and date.
                name = ""
                try:
                    name = card.inner_text(timeout=1_000).strip().splitlines()[0]
                except Exception:
                    pass
                # Fallback: use any visible text containing .pdf
                if not name.lower().endswith(".pdf"):
                    txt = card.inner_text(timeout=1_000)
                    m = re.search(r"[\w\-.]+\.pdf", txt, re.IGNORECASE)
                    if m:
                        name = m.group(0)
                if not name:
                    name = f"document_{i}.pdf"
                results.append({"filename": name, "download_locator": card})
            except Exception:
                continue
        return results

    def download_pdf(self, card_locator, target_dir: Path) -> Optional[Path]:
        """Click a PDF card and wait for the download to finish."""
        target_dir.mkdir(parents=True, exist_ok=True)
        try:
            with self.page.expect_download(timeout=60_000) as dl_info:
                card_locator.click()
            download = dl_info.value
            # Sanitise the filename.
            safe = re.sub(r"[^\w\-.]", "_", download.suggested_filename or "document.pdf")
            if not safe.lower().endswith(".pdf"):
                safe += ".pdf"
            dest = target_dir / safe
            download.save_as(str(dest))
            print(f"  Downloading PDF... -> {dest.name}")
            return dest
        except Exception as e:
            log.warning("Download failed: %s", e)
            return None

    # -- high level --------------------------------------------------------
    def scrape_chat(self, target: str, db: Database, history_days: int = DEFAULT_HISTORY_DAYS) -> int:
        """Open `target` chat, scroll back, download PDFs, record metadata. Returns count saved."""
        if not self.search_and_open_chat(target):
            return 0
        info = self.get_active_chat_info()
        print(f"  Chat opened: name={info['name']!r} phone={info['phone']!r}")
        target_date = datetime.now() - timedelta(days=history_days)
        self.scroll_back_until(target_date)
        saved = 0
        cards = self.collect_pdf_links()
        print(f"  Found {len(cards)} PDF attachment card(s).")
        for c in cards:
            dest = self.download_pdf(c["download_locator"], PDF_DIR)
            if dest is None:
                continue
            date_str = datetime.now().isoformat(timespec="seconds")
            db.upsert_pdf(
                customer_name=info["name"],
                phone_number=info["phone"],
                message_date=date_str,
                pdf_filename=dest.name,
                local_file_path=str(dest),
                file_hash=file_hash(dest),
            )
            saved += 1
        db.set_sync_state(target)
        return saved

    def incremental_sync(self, targets: list[str], db: Database, days: int = 3) -> int:
        """Re-open each chat and only look at the most recent few days."""
        print(f"Incremental sync over last {days} day(s) for {len(targets)} chat(s).")
        total = 0
        for t in targets:
            total += self.scrape_chat(t, db, history_days=days)
        return total


# ============================================================================
# CLI / menu
# ============================================================================
def _print_rows(rows: Iterable[sqlite3.Row]) -> None:
    rows = list(rows)
    if not rows:
        print("  (no records)")
        return
    for r in rows:
        print("  -----------------------------------------------------------")
        print(f"  Customer : {r['customer_name']}")
        print(f"  Phone    : {r['phone_number']}")
        print(f"  Date     : {r['message_date']}")
        print(f"  PDF      : {r['pdf_filename']}")
        print(f"  Path     : {r['local_file_path']}")
        plates = json.loads(r["plates"] or "[]")
        vins   = json.loads(r["vins"]   or "[]")
        if plates:
            print(f"  Plates   : {', '.join(plates)}")
        if vins:
            print(f"  VINs     : {', '.join(vins)}")


def menu() -> None:
    db = Database()
    print("\n=== WhatsApp Plate Finder ===")
    while True:
        print(
            "\nOptions:"
            "\n  1 - Login & full scrape of customer chat list"
            "\n  2 - Run PDF extraction pipeline"
            "\n  3 - Search by plate number"
            "\n  4 - Search by date (YYYY-MM-DD)"
            "\n  5 - Search by customer name"
            "\n  6 - Run incremental sync"
            "\n  7 - Show full index"
            "\n  0 - Quit"
        )
        choice = input("Select> ").strip()

        if choice == "0":
            print("Bye.")
            break

        elif choice == "1":
            targets = input("Enter comma-separated phone numbers / names: ").strip()
            if not targets:
                continue
            try:
                scraper = WhatsAppScraper(headless=HEADLESS)
            except Exception:
                continue
            with scraper:
                scraper.open()
                total = 0
                for t in [x.strip() for x in targets.split(",") if x.strip()]:
                    total += scraper.scrape_chat(t, db)
                print(f"\nSaved {total} PDF(s) to database.")
            # auto-run extraction right after a scrape
            run_pdf_extraction_pipeline(db)

        elif choice == "2":
            run_pdf_extraction_pipeline(db)

        elif choice == "3":
            plate = input("Plate number: ").strip()
            _print_rows(db.search_by_plate(plate))

        elif choice == "4":
            date_str = input("Date (YYYY-MM-DD): ").strip()
            _print_rows(db.search_by_date(date_str))

        elif choice == "5":
            name = input("Customer name (partial ok): ").strip()
            _print_rows(db.search_by_customer(name))

        elif choice == "6":
            raw = input("Comma-separated chat keys (blank = same as last full scrape): ").strip()
            if not raw:
                print("Please provide chat keys.")
                continue
            keys = [x.strip() for x in raw.split(",") if x.strip()]
            try:
                scraper = WhatsAppScraper(headless=HEADLESS)
            except Exception:
                continue
            with scraper:
                scraper.open()
                n = scraper.incremental_sync(keys, db, days=3)
                print(f"Incremental sync saved {n} new PDF(s).")
            run_pdf_extraction_pipeline(db)

        elif choice == "7":
            _print_rows(db.get_all())

        else:
            print("Unknown option.")


# ============================================================================
# Entry point
# ============================================================================
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WhatsApp Web PDF/plate finder")
    p.add_argument("--headless", action="store_true", help="run browser headless")
    p.add_argument("--menu", action="store_true", help="open the interactive menu (default)")
    return p.parse_args()


def main() -> None:
    global HEADLESS
    args = _parse_args()
    if args.headless:
        HEADLESS = True
    try:
        menu()
    except KeyboardInterrupt:
        print("\nInterrupted.")


if __name__ == "__main__":
    main()
