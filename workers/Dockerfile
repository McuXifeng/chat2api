FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy

WORKDIR /app

COPY workers/requirements.txt /app/workers/requirements.txt
RUN pip install --no-cache-dir -r workers/requirements.txt

# Reuse gateway-side modules so chunk format stays identical.
COPY chatgpt /app/chatgpt
COPY utils /app/utils
COPY workers /app/workers
COPY version.txt /app/version.txt

ENV PYTHONPATH=/app
ENV BROWSER_PROFILES=/var/lib/browser-profiles
RUN mkdir -p /var/lib/browser-profiles

CMD ["python", "-u", "workers/main.py"]
