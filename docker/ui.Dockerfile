# ==============================================================================
# Dockerfile cho giao diện Dashboard Batch Gold
# Build context: thư mục gốc dự án (.)
# ==============================================================================
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

COPY requirements/ui.txt /tmp/requirements/ui.txt
RUN pip install --no-cache-dir -r /tmp/requirements/ui.txt \
    && rm -rf /tmp/requirements

COPY ui /app/ui
COPY batch/gold/dashboard.py /app/batch/gold/dashboard.py

CMD ["streamlit", "run", "ui/app.py", "--server.address=0.0.0.0", "--server.headless=true"]
