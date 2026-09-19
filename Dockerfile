FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright + Chromium (solo para /api/production-mockup — captura de
# screenshot del HTML final generado). Instala dependencias del sistema
# necesarias para correr Chromium headless en Debian slim.
RUN playwright install --with-deps chromium

# Copiar código
COPY main.py .

# Puerto del servicio
EXPOSE 8765

# Variables de entorno requeridas (se configuran en EasyPanel)
ENV PIPEDRIVE_API_KEY=""
ENV TELEGRAM_BOT_TOKEN=""
ENV TELEGRAM_HOME_CHANNEL="8808084550"
ENV PORT="8765"

# Healthcheck para EasyPanel
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:${PORT}/health', timeout=3)"

CMD ["python3", "main.py"]
