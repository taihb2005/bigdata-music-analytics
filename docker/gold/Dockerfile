FROM python:3.12-slim AS base
WORKDIR /app
ENV PYTHONUNBUFFERED=1

FROM base AS worker
COPY batch/gold/requirements.txt batch/gold/requirements-api.txt /tmp/gold/
RUN pip install --no-cache-dir -r /tmp/gold/requirements.txt
COPY batch/__init__.py /app/batch/__init__.py
COPY batch/gold /app/batch/gold
CMD ["python", "-m", "batch.gold", "watch"]

FROM base AS api
COPY batch/gold/requirements-api.txt /tmp/gold-api.txt
RUN pip install --no-cache-dir -r /tmp/gold-api.txt
COPY batch/__init__.py /app/batch/__init__.py
COPY batch/gold/__init__.py batch/gold/catalog.py batch/gold/parquet.py batch/gold/api.py /app/batch/gold/
CMD ["python", "-m", "batch.gold.api"]

FROM base AS dashboard
COPY batch/gold/requirements-ui.txt /tmp/requirements-ui.txt
RUN pip install --no-cache-dir -r /tmp/requirements-ui.txt
COPY batch/gold/dashboard.py /app/batch/gold/dashboard.py
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
CMD ["streamlit", "run", "batch/gold/dashboard.py", "--server.address=0.0.0.0", "--server.headless=true"]
