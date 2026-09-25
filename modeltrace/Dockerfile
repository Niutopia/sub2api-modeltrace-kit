FROM python:3.12-slim@sha256:44ff437bba879d4941b710a369a8f19266aea34b29002807f0c487fabc9eec9b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MODELTRACE_CONFIG=/run/modeltrace/config.json \
    MODELTRACE_DB_PATH=/data/modeltrace.sqlite3

WORKDIR /app

COPY requirements.lock.txt ./requirements.lock.txt
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.lock.txt \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin modeltrace \
    && mkdir -p /data /run/modeltrace \
    && chown -R modeltrace:modeltrace /data /run/modeltrace /app

COPY --chown=modeltrace:modeltrace app.py ./app.py
COPY --chown=modeltrace:modeltrace modeltrace ./modeltrace
COPY --chown=modeltrace:modeltrace LICENSE ./LICENSE

USER modeltrace
EXPOSE 8081
VOLUME ["/data", "/run/modeltrace"]

# One application worker owns the one ModelTrace queue worker. Do not scale this
# container horizontally without an external singleton/lease.
CMD ["gunicorn", "--workers", "1", "--threads", "4", "--bind", "0.0.0.0:8081", "--timeout", "30", "--graceful-timeout", "5", "--access-logfile", "-", "--error-logfile", "-", "app:create_app()"]
