# Linux Offline Install

These offline assets target Linux x86_64 with Python 3.10.

## Install Python dependencies

From the project root on the Linux server:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install --no-index --find-links=offline_packages_linux_x86_64_py310 -r requirements.txt
```

## Restore Playwright browser cache

Copy the bundled Linux browser cache into the target user's Playwright cache directory:

```bash
mkdir -p ~/.cache/ms-playwright
cp -a ms-playwright_linux_x86_64/. ~/.cache/ms-playwright/
```

If the server runs the service under another user, run the copy command as that user or copy to that user's home directory.

## Start the API

```bash
source .venv/bin/activate
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Then open:

```text
http://SERVER_IP:8000/docs
```

Note: Playwright Chromium also needs Linux system libraries. If Chromium fails to launch with missing `.so` errors, install the missing OS packages through your company's offline package process.
