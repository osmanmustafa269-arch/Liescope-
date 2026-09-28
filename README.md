# LieScope v6.0

LieScope is a local-first scientific publication intelligence app for exploring publications associated with Stein Atle Lie.

## What v6 adds
- OpenAlex + Crossref + PubMed + Europe PMC + NVA discovery/reconciliation.
- Exact author-position classification with review queue and source provenance.
- A full-screen in-app paper workspace: Overview, Abstract, PDF Reader, Figures, Tables, Authors, Sources.
- Mozilla PDF.js reader with thumbnails, Previous/Next, page jump, zoom, fit width/page, rotation, fullscreen, text selection, PDF text search, keyboard navigation, swipe navigation, and remembered last page.
- Local lawful PDF library with checksum and persistent storage.
- OA PDF discovery order: PubMed Central -> OpenAlex OA PDF -> optional Unpaywall when `LIESCOPE_CONTACT_EMAIL` is configured -> user upload.
- Actual figures/tables from structured PMC full text first, with saved-PDF extraction fallback using PyMuPDF.
- PDF-derived figure/table confidence labels. Low-confidence table extraction is shown as a page crop rather than invented cells.
- "Paper at a glance" generated only from explicit labelled abstract sections and verified metadata.
- Favorites, reading history/continue-reading page state, CSV/RIS/BibTeX export.
- Monthly server-side refresh on the 1st at 06:00 Europe/Oslo while the backend is deployed.

## Legal access rule
LieScope never bypasses publisher paywalls, authentication, DRM, or access controls. Automatic PDF saving is limited to lawful open-access sources exposed by scholarly metadata. The upload option is for a PDF the user already lawfully possesses.

## Persistent data
- Windows: `%LOCALAPPDATA%\LieScope\`
- macOS/Linux: `~/.liescope/`

The folder contains the SQLite database, cached PDFs, and derived figure/table images.

## Run on Windows
1. Extract the ZIP.
2. Double-click `run_windows.bat`.
3. Open `http://localhost:8000`.
4. Click **Run live update**.
5. Click **READ PAPER IN LIESCOPE**.

## Run on macOS/Linux
```bash
chmod +x run_mac_linux.sh
./run_mac_linux.sh
```

## Optional Unpaywall support
Set a contact email before running:
```bash
LIESCOPE_CONTACT_EMAIL=you@example.com
```
On Windows PowerShell:
```powershell
$env:LIESCOPE_CONTACT_EMAIL="you@example.com"
```

## Tests
```bash
python -m pytest -q
```

## Notes
- PDF.js is loaded from cdnjs. If the CDN is blocked, the stored PDF can still be opened separately.
- PubMed Central structured figures/tables are preferred over PDF extraction.
- PDF extraction is layout-based, not OCR-based; low-confidence results are labelled.

## Open on a phone on the same Wi-Fi
On Windows run `run_windows_phone.bat`, find the PC IPv4 address with `ipconfig`, then open `http://PC-IP:8000` on the phone. On macOS/Linux use `run_mac_linux_phone.sh`.

## Render deployment
This distribution includes a production-oriented Render Blueprint in `render.yaml` and a phone deployment walkthrough in `RENDER_DEPLOY_PHONE.md`.
Use `render.yaml` for persistent PDFs/database. `render-free.yaml` is only a temporary preview because Render Free does not preserve local files across restarts.
