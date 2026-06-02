# Nimbus Commerce Platform

## Getting started

### Prerequisites

- Python **3.12+** (we pin 3.12.7 via `.python-version` for pyenv users)
- Docker + Docker Compose v2
- GNU Make (or run the equivalent commands by hand — see the `Makefile`)

### One-time setup

```bash
# Option A: pyenv (recommended)
pyenv install $(cat .python-version)
pyenv local $(cat .python-version)

# Option B: system Python
python3.12 -m venv .venv

# Then:
source .venv/bin/activate
make install          # equivalent to: pip install -e ".[dev]"
