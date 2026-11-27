FROM python:3.12-slim AS base
WORKDIR /app
ENV PYTHONUNBUFFERED=1

FROM base AS worker
COPY batch/gold/requirements*.txt /tmp/gold/
RUN pip install --no-cache-dir -r /tmp/gold/requirements.txt
COPY batch/__init__.py /app/batch/__init__.py
COPY batch/gold /app/batch/gold
CMD ["python", "-m", "batch.gold", "watch"]

FROM base AS dashboard
RUN pip install --no-cache-dir "streamlit>=1.40,<2" pandas
COPY streamlit_app.py /app/streamlit_app.py
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
CMD ["streamlit", "run", "streamlit_app.py", "--server.address=0.0.0.0", "--server.headless=true"]
