# Repository Guidelines

## Project Structure & Module Organization
This repo is a Python market-making system for the Lighter DEX. Key files live in the root:
- `gather_lighter_data.py` streams order book and trades to `lighter_data/` (Parquet).
- `calculate_avellaneda_parameters.py` writes model parameters to `params/`.
- `market_maker_v2.py` places and manages orders.
- Supporting modules: `utils.py`, `volatility.py`, `intensity.py`, `backtest.py`.
- Operational assets: `docker-compose.yml`, `Dockerfile`, `requirements.txt`, `.env.example`, `config.json`.
- `DOC/` holds protocol notes; `OLD/` contains legacy scripts.

## Build, Test, and Development Commands
Install dependencies:
```bash
pip install -r requirements.txt
```
Build and run services with Docker:
```bash
docker-compose build
docker-compose up -d
docker-compose down
```
Run individual scripts locally:
```bash
python gather_lighter_data.py
python calculate_avellaneda_parameters.py --minutes 15
python market_maker_v2.py
```
Smoke-check the data collector:
```bash
python test_runner.py
```

## Coding Style & Naming Conventions
- Python uses 4-space indentation, snake_case for functions and variables, and UPPER_CASE for constants.
- Keep configuration in `.env` and `config.json`; avoid hardcoding credentials or symbols.
- No formatter or linter is enforced; keep changes PEP 8 friendly and consistent with existing modules.

## Testing Guidelines
- There is no formal test framework configured.
- Use `test_runner.py` as a quick smoke test for data collection and `check_parquet.py` to validate stored data.
- If you add tests, document how to run them in this file.
 - Unit tests (market maker logic): `python -m unittest discover -s tests -p "test_*.py"`.

## Commit & Pull Request Guidelines
- Recent commits use short, imperative messages like "Add ...", "Update ...", or "Implement ...".
- PRs should include: a concise summary, affected scripts, configuration changes (`.env`, `config.json`, `docker-compose.yml`), and any safety or risk notes for live trading.

## Security & Configuration Tips
- Never commit real API keys or private keys; use `.env` and update `.env.example` when adding variables.
- Treat `lighter.pem` and any key material as sensitive and keep them out of PRs.
