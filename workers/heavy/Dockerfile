FROM python:3.11-slim

WORKDIR /app

# Dependencias del sistema para psycopg2 + Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    # Playwright necesita estas libs para Chromium
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instalar solo Chromium (más liviano que todos los browsers)
RUN playwright install chromium

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]