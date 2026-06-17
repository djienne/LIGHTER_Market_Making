FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    ALLOW_PYTHON_FALLBACK=0 \
    LIGHTER_MM_CONFIG=/app/config.json \
    LOG_DIR=/app/logs

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install -r requirements.txt

COPY . .

RUN python setup_cython.py build_ext --inplace \
    && python - <<'PY'
from _vol_obi_fast import CBookSide, RollingStats, VolObiCalculator
from _vol_obi_fast import dynamic_max_position_fast, price_change_bps_fast
print("cython import ok")
PY

CMD ["python", "-u", "market_maker_v2.py", "--symbol", "BTC", "--live"]
