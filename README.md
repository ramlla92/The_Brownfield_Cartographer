# Brownfield Cartographer 🗺️

> **A multi-agent codebase intelligence system for rapid FDE onboarding in production environments.**

In the first 72 hours of a brownfield engagement, the FDE faces Navigation Blindness, Contextual Amnesia, Dependency Opacity, and Silent Debt. The Brownfield Cartographer builds instruments to make codebases legible—producing a living, queryable map of any production codebase's architecture, data flows, and semantic structure.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Brownfield Cartographer                    │
│                                                              │
│  ┌───────────┐   ┌─────────────┐   ┌─────────────────────┐  │
│  │ Surveyor  │   │ Hydrologist │   │    Semanticist       │  │
│  │ (Static)  │──▶│ (Lineage)   │──▶│    (LLM)            │  │
│  └───────────┘   └─────────────┘   └─────────────────────┘  │
│       │               │                      │               │
│       └───────────────┴──────────────────────┘               │
│                            │                                 │
│                    ┌───────▼───────┐                         │
│                    │   Archivist   │  → .cartography/         │
│                    └───────────────┘                         │
│                            │                                 │
│                    ┌───────▼───────┐                         │
│                    │   Navigator   │  → query interface       │
│                    └───────────────┘                         │
└──────────────────────────────────────────────────────────────┘
```

| Agent | Role |
|---|---|
| **Surveyor** | tree-sitter AST parsing, module import graph, PageRank, git velocity, dead code |
| **Hydrologist** | Data lineage DAG — Python/pandas + sqlglot SQL + YAML/Airflow/dbt |
| **Semanticist** | LLM purpose statements, domain clustering, Five FDE Day-One Q&A |
| **Archivist** | `CODEBASE.md`, `onboarding_brief.md`, JSON graphs, `cartography_trace.jsonl` |
| **Navigator** | LangGraph query agent: `find_implementation`, `trace_lineage`, `blast_radius`, `explain_module` |

---

## Installation

### Prerequisites
- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/) (recommended) or pip
- git

### Setup

```bash
# Clone the repo
git clone https://github.com/your-org/brownfield-cartographer
cd brownfield-cartographer

# Install dependencies with uv
uv sync

# Copy and configure environment
cp .env.example .env
# Edit .env — add your GOOGLE_API_KEY or OPENAI_API_KEY
```

---

## Usage

### `analyze` — Run the full pipeline

```bash
# Analyse a local repository
cartographer analyze /path/to/repo

# Analyse a GitHub repository (auto-cloned)
cartographer analyze https://github.com/dbt-labs/jaffle_shop

# Skip LLM (no API key needed — faster, produces structural analysis only)
cartographer analyze ./repo --no-llm

# Custom output directory
cartographer analyze ./repo --output ./my-cartography
```

**Outputs written to `.cartography/` (or `--output`):**

| File | Description |
|---|---|
| `CODEBASE.md` | Living context file — inject into any AI coding agent |
| `onboarding_brief.md` | Five FDE Day-One answers with evidence citations |
| `module_graph.json` | Serialized module dependency graph (NetworkX node-link format) |
| `lineage_graph.json` | Serialized data lineage DAG |
| `cartography_trace.jsonl` | Audit log of every analysis action |

---

### `query` — Interactive Navigator

```bash
# Explain a module
cartographer query ./repo --tool explain_module --arg src/agents/surveyor.py

# Find where business logic lives
cartographer query ./repo --tool find_implementation --arg "revenue calculation"

# Trace upstream data sources
cartographer query ./repo --tool trace_lineage --arg orders --direction upstream

# Blast radius analysis
cartographer query ./repo --tool blast_radius --arg src/transforms/revenue.py
```

---

## Makefile Shortcuts

```bash
make install            # Install all dependencies
make install-dev        # Install with dev extras
make test               # Run tests
make lint               # Ruff + mypy
make format             # Black + ruff --fix

make analyze-jaffle     # Analyse dbt jaffle_shop (no LLM)
make analyze-airflow    # Analyse Apache Airflow examples (no LLM)
make analyze-self       # Self-referential: analyse this repo
```

---

## Project Structure

```
The_Brownfield_Cartographer/
├── src/
│   ├── cli.py                     # Typer CLI entrypoint (analyze + query)
│   ├── orchestrator.py            # Pipeline wiring (Surveyor→Hydro→Semantic→Archive)
│   ├── models/
│   │   ├── nodes.py               # Pydantic: ModuleNode, DatasetNode, FunctionNode, ...
│   │   ├── edges.py               # Pydantic: ImportEdge, ProducesEdge, ConsumesEdge, ...
│   │   └── graph.py               # Pydantic: ModuleGraph, DataLineageGraph, CartographyResult
│   ├── analyzers/
│   │   ├── tree_sitter_analyzer.py  # Multi-language AST parsing + LanguageRouter
│   │   ├── sql_lineage.py           # sqlglot-based SQL dependency extraction
│   │   └── dag_config_parser.py     # Airflow DAG + dbt schema.yml parser
│   ├── agents/
│   │   ├── surveyor.py            # Agent 1: Static structure
│   │   ├── hydrologist.py         # Agent 2: Data lineage DAG
│   │   ├── semanticist.py         # Agent 3: LLM purpose + domain clustering
│   │   ├── archivist.py           # Agent 4: Living artifact generation
│   │   └── navigator.py           # Agent 5: LangGraph query interface
│   └── graph/
│       └── knowledge_graph.py     # NetworkX wrapper: PageRank, SCC, blast_radius
├── tests/                         # pytest test suite
├── .cartography/                  # Analysis outputs (gitignored by default)
├── RECONNAISSANCE.md              # Manual Day-One analysis template
├── pyproject.toml                 # Dependencies (uv)
├── Makefile                       # Common workflow shortcuts
└── .env.example                   # Environment variable template
```

---

## The Five FDE Day-One Questions

The Cartographer is designed to answer these questions for any codebase, with file:line evidence:

1. **What is the primary data ingestion path?**
2. **What are the 3-5 most critical output datasets/endpoints?**
3. **What is the blast radius if the most critical module fails?**
4. **Where is the business logic concentrated vs. distributed?**
5. **What has changed most frequently in the last 90 days?**

---

## Target Codebases

| Codebase | Status |
|---|---|
| [dbt jaffle_shop](https://github.com/dbt-labs/jaffle_shop) | Primary target (Phase 1) |
| [Apache Airflow examples](https://github.com/apache/airflow) | Primary target (Phase 2) |
| This repo (self-audit) | Phase 4 validation |

---

## Evaluation Phases

| Phase | What's built |
|---|---|
| **Phase 0** | Manual RECONNAISSANCE.md for chosen target |
| **Phase 1** | Surveyor: module graph, PageRank, git velocity, dead code |
| **Phase 2** | Hydrologist: SQL lineage, Python dataflow, YAML/DAG config |
| **Phase 3** | Semanticist: purpose statements, domain clustering, Day-One Q&A |
| **Phase 4** | Archivist + Navigator: living artifacts + query interface |

---

## License

MIT
