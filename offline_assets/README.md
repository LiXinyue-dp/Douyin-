# Offline Install Assets

This directory contains the files needed to install and run this project on an offline Windows server.

## Install Python dependencies

From the project root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --no-index --find-links=offline_assets\offline_packages -r requirements.txt
```

## Restore Playwright browser cache

Copy the bundled browser cache to the target user's local Playwright cache path:

```powershell
$target = "$env:LOCALAPPDATA\ms-playwright"
New-Item -ItemType Directory -Force -Path $target | Out-Null
Copy-Item -Recurse -Force offline_assets\ms-playwright\* $target
```

## Start the API

```powershell
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Then open:

```text
http://SERVER_IP:8000/docs
```
