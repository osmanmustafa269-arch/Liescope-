# Deploy LieScope v6 to Render from your phone

## Recommended: persistent version
This uses `render.yaml`. It keeps the SQLite database, uploaded PDFs, downloaded open-access PDFs, extracted figures, and extracted tables under `/data` on a Render persistent disk.

### What you need
1. A GitHub account.
2. A Render account.
3. This project uploaded to a GitHub repository.

### Deploy
1. On GitHub, create a new repository named `liescope`.
2. Upload **all files from this package** to the repository root. `render.yaml` must be at the top level.
3. Open Render: https://dashboard.render.com/
4. Tap **New +** → **Blueprint**.
5. Connect GitHub if prompted.
6. Select your `liescope` repository.
7. Render detects `render.yaml`.
8. When prompted for `LIESCOPE_CONTACT_EMAIL`, enter your email. This is used only for scholarly APIs that request a contact email.
9. Review the service and create/deploy it.
10. Wait for the deploy to become **Live**.
11. Open the generated `https://<name>.onrender.com` URL on your phone.
12. In LieScope, run the first live publication refresh.

## Free preview
`render-free.yaml` is provided only for testing. Free Render web services have an ephemeral filesystem, so uploaded PDFs, SQLite data, figures, and tables can disappear after restart/spin-down/redeploy. Do not use the free preview as the final PDF library.

## Health check
Render checks `/api/health`. A healthy deployment should return JSON containing `"ok": true`.

## Storage
Persistent deployment stores:
- database: `/data/liescope.db`
- PDFs: `/data/pdfs`
- extracted figures/tables: `/data/extracted`

## Updating later
Push updated files to the same GitHub repository. Render will redeploy automatically when Git integration is enabled. The `/data` disk remains separate from the application image, so stored PDFs/database survive deploys.
