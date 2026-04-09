import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent / "config.json"


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        sys.exit(f"[ERROR] config.json not found at {CONFIG_PATH}")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f)

    # Validate required fields
    required = [
        ("auth", "base_url"),
        ("auth", "username"),
        ("auth", "password"),
        ("target", "space_key"),
    ]
    for section, key in required:
        val = cfg.get(section, {}).get(key)
        if not val:
            sys.exit(f"[ERROR] config.json: '{section}.{key}' is required and must not be empty.")

    return cfg


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(cfg: dict) -> logging.Logger:
    log_cfg = cfg.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_cfg.get("log_to_file") and log_cfg.get("log_file"):
        handlers.append(logging.FileHandler(log_cfg["log_file"], encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )
    return logging.getLogger("conf-pdf-exporter")


# ---------------------------------------------------------------------------
# HTTP Session
# ---------------------------------------------------------------------------

def build_session(cfg: dict) -> requests.Session:
    auth_cfg = cfg["auth"]
    ssl_cfg = cfg.get("ssl", {})
    safety = cfg.get("safety", {})

    session = requests.Session()
    session.auth = (auth_cfg["username"], auth_cfg["password"])

    # SSL
    if not ssl_cfg.get("verify_ssl", True):
        logging.warning("SSL verification is DISABLED. Use only on trusted networks.")
        session.verify = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    elif ssl_cfg.get("ca_bundle_path"):
        session.verify = ssl_cfg["ca_bundle_path"]

    # Retry on transient network errors only (not on 4xx/5xx job failures)
    retry = Retry(
        total=safety.get("max_retries", 3),
        backoff_factor=safety.get("retry_backoff_factor", 2),
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


# ---------------------------------------------------------------------------
# Confluence helpers
# ---------------------------------------------------------------------------

def test_connection(session: requests.Session, base_url: str, timeout: int, log: logging.Logger):
    log.info("Testing connection to Confluence...")
    url = f"{base_url}/rest/api/space"
    try:
        r = session.get(url, params={"limit": 1}, timeout=timeout)
        r.raise_for_status()
        log.info("Connection OK.")
    except requests.exceptions.ConnectionError as e:
        sys.exit(f"[ERROR] Cannot reach Confluence at {base_url}: {e}")
    except requests.exceptions.HTTPError as e:
        sys.exit(f"[ERROR] Auth or API error: {e} — check username/password and base_url.")


def resolve_page_id(session: requests.Session, base_url: str, space_key: str,
                    page_title: str, timeout: int, log: logging.Logger) -> str:
    log.info(f"Looking up page ID for title: '{page_title}' in space '{space_key}'...")
    url = f"{base_url}/rest/api/content"
    r = session.get(url, params={
        "type": "page",
        "spaceKey": space_key,
        "title": page_title,
        "expand": "version",
    }, timeout=timeout)
    r.raise_for_status()
    results = r.json().get("results", [])
    if not results:
        sys.exit(f"[ERROR] No page found with title '{page_title}' in space '{space_key}'.")
    if len(results) > 1:
        log.warning(f"Multiple pages found with title '{page_title}', using the first one.")
    page_id = results[0]["id"]
    log.info(f"Resolved page ID: {page_id}")
    return page_id


# ---------------------------------------------------------------------------
# Scroll PDF Export
# ---------------------------------------------------------------------------

def build_export_payload(cfg: dict, page_id: str) -> dict:
    target = cfg["target"]
    sp = cfg["scroll_pdf"]

    payload = {
        "spaceKey": target["space_key"],
        "scope": sp["export"].get("scope", "DESCENDANTS"),
    }

    # Root / anchor page
    if page_id:
        payload["rootPageId"] = page_id

    # Optional export filters
    for key in ("templateId", "versionCommentFilter", "labelFilter", "ancestorId"):
        val = sp["export"].get(key)
        if val is not None:
            payload[key] = val

    # Rendering options
    rendering = sp.get("rendering", {})
    rendering_map = {
        "includeComments": "includeComments",
        "includeUnresolvedComments": "includeUnresolvedComments",
        "includeAttachments": "includeAttachments",
        "includePageBreaks": "includePageBreaks",
        "pageBreakBetweenPages": "pageBreakBetweenPages",
        "screenshotsEnabled": "screenshotsEnabled",
        "screenshotWidth": "screenshotWidth",
        "watermarkText": "watermarkText",
        "watermarkEnabled": "watermarkEnabled",
        "showPageNumbers": "showPageNumbers",
        "showTableOfContents": "showTableOfContents",
        "showTitle": "showTitle",
        "showAuthor": "showAuthor",
        "showDate": "showDate",
    }
    for cfg_key, api_key in rendering_map.items():
        val = rendering.get(cfg_key)
        if val is not None:
            payload[api_key] = val

    # Layout
    layout = sp.get("layout", {})
    layout_map = {
        "pageSize": "pageSize",
        "orientation": "orientation",
        "marginTopMm": "marginTopMm",
        "marginBottomMm": "marginBottomMm",
        "marginLeftMm": "marginLeftMm",
        "marginRightMm": "marginRightMm",
    }
    for cfg_key, api_key in layout_map.items():
        val = layout.get(cfg_key)
        if val is not None:
            payload[api_key] = val

    return payload


SCROLL_PDF_BASE = "plugins/servlet/scroll-pdf/api/exports"


def start_export_job(session: requests.Session, base_url: str, payload: dict,
                     timeout: int, log: logging.Logger) -> tuple[str, str | None]:
    url = f"{base_url}/{SCROLL_PDF_BASE}"
    log.info("Starting Scroll PDF export job...")
    log.debug(f"POST {url}")
    log.debug(f"Payload: {json.dumps(payload, indent=2)}")

    r = session.post(url, json=payload, timeout=timeout)

    if r.status_code == 401:
        sys.exit("[ERROR] Authentication failed (401). Check username and password.")
    if r.status_code == 403:
        sys.exit("[ERROR] Permission denied (403). Your account may not have export rights.")
    if r.status_code == 404:
        sys.exit(
            "[ERROR] Scroll PDF endpoint not found (404).\n"
            f"  Tried: {url}\n"
            "  Confirm the Scroll PDF Exporter plugin is installed and the base_url is correct."
        )
    r.raise_for_status()

    data = r.json()
    log.debug(f"Response: {data}")

    # API returns the job as a resource — id or jobId field, and sometimes a self/download link
    job_id = (
        data.get("id")
        or data.get("jobId")
        or data.get("exportId")
    )
    if not job_id:
        sys.exit(f"[ERROR] Export job started but no job ID returned. Response: {data}")

    # Some versions return a ready download URL immediately
    download_url = data.get("downloadUrl") or data.get("download")

    log.info(f"Export job started. Job ID: {job_id}")
    return str(job_id), download_url


def poll_job(session: requests.Session, base_url: str, job_id: str, cfg: dict,
             log: logging.Logger) -> str | None:
    """Returns a download URL if the API provides one in the status response."""
    safety = cfg.get("safety", {})
    poll_interval = safety.get("poll_interval_sec", 10)
    max_attempts = safety.get("max_poll_attempts", 30)
    timeout = safety.get("request_timeout_sec", 60)

    url = f"{base_url}/{SCROLL_PDF_BASE}/{job_id}"

    for attempt in range(1, max_attempts + 1):
        log.info(f"Checking job status... (attempt {attempt}/{max_attempts})")
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        log.debug(f"Status response: {data}")

        status = (data.get("status") or data.get("state") or "").upper()
        progress = data.get("progress") or data.get("percentage") or ""
        if progress:
            log.info(f"Status: {status} — Progress: {progress}%")
        else:
            log.info(f"Status: {status}")

        if status in ("COMPLETED", "DONE", "SUCCESS", "FINISHED"):
            log.info("Export job completed.")
            return data.get("downloadUrl") or data.get("download")

        if status in ("FAILED", "ERROR", "CANCELLED"):
            error_msg = data.get("errorMessage") or data.get("message") or "No details provided."
            sys.exit(f"[ERROR] Export job failed. Status: {status}. Message: {error_msg}")

        if attempt < max_attempts:
            log.info(f"Waiting {poll_interval}s before next check...")
            time.sleep(poll_interval)

    sys.exit(
        f"[ERROR] Export job did not complete after {max_attempts} attempts "
        f"({max_attempts * poll_interval}s total). "
        "Consider increasing 'safety.max_poll_attempts' or 'safety.poll_interval_sec' in config.json."
    )


def download_pdf(session: requests.Session, base_url: str, job_id: str,
                 output_path: Path, timeout: int, log: logging.Logger,
                 download_url: str | None = None) -> None:
    # Use API-provided URL if available, otherwise fall back to conventional path
    url = download_url or f"{base_url}/{SCROLL_PDF_BASE}/{job_id}/download"
    log.info(f"Downloading PDF from {url}...")

    # Check disk space (rough estimate: 500MB free required)
    free_bytes = _free_disk_bytes(output_path.parent)
    if free_bytes is not None and free_bytes < 500 * 1024 * 1024:
        log.warning(f"Low disk space: {free_bytes // (1024*1024)} MB free. Proceeding anyway.")

    with session.get(url, stream=True, timeout=timeout) as r:
        if r.status_code == 404:
            sys.exit("[ERROR] Download URL not found (404). The job may have expired.")
        r.raise_for_status()

        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        chunk_size = 1024 * 1024  # 1 MB chunks

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to temp file first, rename on success to avoid partial files
        tmp_path = output_path.with_suffix(".tmp")
        try:
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            pct = downloaded * 100 // total
                            print(f"\r  Downloaded: {downloaded // 1024} KB / {total // 1024} KB ({pct}%)", end="", flush=True)
            print()  # newline after progress
            tmp_path.rename(output_path)
        except Exception as e:
            tmp_path.unlink(missing_ok=True)
            raise

    file_size = output_path.stat().st_size
    if file_size < 1024:
        log.warning(f"Downloaded file is suspiciously small ({file_size} bytes). It may be an error response.")
    else:
        log.info(f"PDF saved: {output_path} ({file_size // 1024} KB)")


def _free_disk_bytes(path: Path):
    try:
        import shutil
        return shutil.disk_usage(path).free
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Output path
# ---------------------------------------------------------------------------

def build_output_path(cfg: dict) -> Path:
    out_cfg = cfg.get("output", {})
    output_dir = Path(out_cfg.get("output_dir", "./output"))
    pattern = out_cfg.get("filename_pattern", "{space_key}_{datetime}")
    dt_format = out_cfg.get("datetime_format", "%Y-%m-%d_%H-%M-%S")

    space_key = cfg["target"]["space_key"]
    dt_str = datetime.now().strftime(dt_format)
    filename = pattern.format(space_key=space_key, datetime=dt_str) + ".pdf"
    # Sanitize filename
    filename = "".join(c if c.isalnum() or c in "-_. " else "_" for c in filename)

    candidate = output_dir / filename
    # Avoid overwriting existing file
    if candidate.exists():
        base = candidate.stem
        suffix = candidate.suffix
        for i in range(2, 100):
            candidate = output_dir / f"{base}_v{i}{suffix}"
            if not candidate.exists():
                break

    return candidate


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    cfg = load_config()
    log = setup_logging(cfg)

    auth = cfg["auth"]
    target = cfg["target"]
    safety = cfg.get("safety", {})
    timeout = safety.get("request_timeout_sec", 60)
    base_url = auth["base_url"].rstrip("/")

    session = build_session(cfg)
    test_connection(session, base_url, timeout, log)

    # Resolve page ID if needed
    page_id = target.get("root_page_id")
    if not page_id and target.get("page_title"):
        page_id = resolve_page_id(
            session, base_url, target["space_key"], target["page_title"], timeout, log
        )

    payload = build_export_payload(cfg, str(page_id) if page_id else None)
    job_id, immediate_download_url = start_export_job(session, base_url, payload, timeout, log)
    polled_download_url = poll_job(session, base_url, job_id, cfg, log)

    download_url = immediate_download_url or polled_download_url
    output_path = build_output_path(cfg)
    download_pdf(session, base_url, job_id, output_path, timeout, log, download_url=download_url)

    log.info("Done.")


if __name__ == "__main__":
    main()
