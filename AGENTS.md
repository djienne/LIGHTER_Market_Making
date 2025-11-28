# Repository Guidelines

Use this playbook to onboard quickly and ship safe changes to the Lighter DEX market-making stack.

## Project Structure & Module Organization
Core scripts live at the repository root: `market_maker.py` executes orders, `gather_lighter_data.py` streams raw market data, and `calculate_avellaneda_parameters.py` computes strategy inputs. Generated artifacts land in `lighter_data/` (CSV history), `params/` (model outputs), and `logs/` (service logs). Docker orchestration resides in `docker-compose.yml` and `Dockerfile`; tweak credentials and symbols in `.env` and `CRYPTO_CONFIGURATION.md` when targeting new markets.

## Build, Test, and Development Commands
- `pip install -r requirements.txt` creates a local Python environment for isolated debugging.
- `docker-compose build` compiles the services with the latest dependencies.
- `docker-compose up -d` starts data collection, parameter calculation, and the market maker in the background.
- `docker-compose logs -f market_maker` tails live trading output for validation; pair with `docker-compose down` to stop services.

## Coding Style & Naming Conventions
Follow PEP 8 with 4-space indentation and descriptive snake_case identifiers. Group configuration constants near the top of each script and document magic numbers with inline comments. Keep new modules under the root or introduce a `src/` folder if the codebase grows; mirror naming between modules and Docker service labels.

## Testing Guidelines
Automated tests are not yet in place, so emphasize reproducible manual checks. After telemetry or strategy changes, run the stack via Docker Compose and confirm parameter JSONs appear in `params/` and orders register correctly in market maker logs. When adding unit tests, prefer `pytest`, store them under `tests/`, and name files `test_<feature>.py` for discoverability.

## Commit & Pull Request Guidelines
Write imperative, present-tense commit subjects ("Add leverage guard"), wrapping bodies at 72 characters. Reference related issues or incident IDs in the body when relevant. Pull requests should summarize scope, list validation steps (commands run, key log excerpts), and attach screenshots for UI-facing tooling like the dashboard scripts. Flag configuration updates in PR descriptions so operators can roll them out safely.

## Security & Configuration Tips
Keep `.env` and `lighter.pem` out of version control; use local environment variables or secret managers. Rotate API keys regularly, and prefer read-only credentials when testing. Document any rate-limit or margin changes in `CRYPTO_CONFIGURATION.md` to keep trading behavior predictable.
