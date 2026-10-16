# Radar: só biblioteca padrão do Python, nada para instalar.
FROM python:3.13-slim
WORKDIR /app
COPY radar.py .
COPY static/ static/
RUN useradd --uid 10001 --no-create-home radar && mkdir -p /app/dados && chown radar /app/dados
USER radar
ENV RADAR_AUTH=proxy RADAR_HOST=0.0.0.0 RADAR_PORTA=1210 RADAR_DADOS=/app/dados RSSHUB_URL=http://rsshub:1200 PYTHONUNBUFFERED=1
HEALTHCHECK --interval=60s --timeout=5s CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:1210/privacidade',timeout=4)"
CMD ["python", "radar.py"]
