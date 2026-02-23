# EY Contact Centre Rapid Value Calculator v7

## What This Is
Flask-based contact centre transformation business case tool with pool-based netting,
stepped realization engine, role-level FTE breakdown, reskilling matrix, NPV/IRR financials,
scenario analysis, sensitivity modeling, and risk register generation.

## Tech Stack
- **Backend**: Python/Flask (stdlib only — no heavy dependencies)
- **Frontend**: Single-page HTML/JS
- **Server**: Gunicorn (production WSGI)
- **Optional**: openpyxl for Excel export

---

## Deploy to Railway

### Step 1: Create a GitHub Repository

```bash
cd ey-contact-centre-rvc
git init
git add .
git commit -m "Contact Centre RVC v7 — initial deploy"
git remote add origin https://github.com/YOUR_USERNAME/ey-contact-centre-rvc.git
git branch -M main
git push -u origin main
```

### Step 2: Deploy on Railway

1. Go to **[railway.app](https://railway.app)** → Sign in with GitHub
2. Click **"New Project"** → **"Deploy from GitHub Repo"**
3. Select your **ey-contact-centre-rvc** repo
4. Railway auto-detects Python and uses the `Procfile`
5. Build completes in ~30 seconds (lightweight dependencies)
6. Go to **Settings** → **Networking** → **"Generate Domain"**
7. Your app is live at: `https://ey-contact-centre-rvc-xxxx.up.railway.app`

### Step 3: Verify

Open the generated URL. You should see the Contact Centre business case interface.

---

## Local Development

```bash
pip install -r requirements.txt
python app.py
# Opens at http://localhost:8081
```

---

## File Structure

```
ey-contact-centre-rvc/
├── app.py              ← Flask app (all logic + API endpoints)
├── requirements.txt    ← Python dependencies
├── Procfile            ← Gunicorn start command for Railway
├── runtime.txt         ← Python version
└── templates/
    └── index.html      ← Single-page frontend
```
