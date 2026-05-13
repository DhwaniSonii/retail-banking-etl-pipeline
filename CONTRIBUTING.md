# Contributing

Thanks for your interest in contributing to the Banking Data Platform.

## Getting Started

```bash
git clone https://github.com/your-username/banking-data-platform.git
cd banking-data-platform
python -m venv venv && source venv/bin/activate
pip install -e ".[dev,notebooks,dashboard]"
```

## Running Tests

```bash
pytest tests/ -v --cov=etl --cov=governance --cov-report=term-missing
```

All PRs must pass the full test suite before merging.

## Code Style

- Max line length: 120 characters
- Docstrings on all public functions and classes
- Type hints on all function signatures
- Run `flake8 etl/ governance/` before pushing

## Project Structure

| Folder | Purpose |
|---|---|
| `etl/extractors/` | Upstream data extraction |
| `etl/transformers/` | Business rules & DQ framework |
| `etl/loaders/` | Database loading |
| `dbt_project/` | Dimensional modeling |
| `governance/` | Lineage, metadata, data dictionary |
| `dashboard/` | Streamlit KPI dashboard |
| `tests/` | pytest unit tests |
| `notebooks/` | Data profiling & EDA |
| `great_expectations/` | Industry-standard DQ checkpoints |

## Branch Strategy

- `main` — production-ready code only
- `develop` — integration branch
- Feature branches: `feature/your-feature-name`

## Decisions Log

See `docs/DECISIONS.md` for architectural decisions and their rationale.
