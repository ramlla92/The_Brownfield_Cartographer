"""
CLI entrypoint: `cartographer`

Subcommands:
    analyze   — Run the full (or partial) analysis pipeline
    query     — Direct tool invocation
    ask       — Chat with the LangGraph Navigator agent

Usage examples:
    cartographer analyze ./path/to/repo
    cartographer analyze https://github.com/dbt-labs/jaffle_shop
    cartographer analyze ./repo --no-llm --output .cartography
    cartographer query ./repo --tool blast_radius --arg src/transforms/revenue.py
    cartographer ask ./repo "Where is the revenue calculation implemented?"
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

import typer
from loguru import logger
from rich.console import Console
from rich.panel import Panel
from rich.progress import track

from src.orchestrator import run_full_pipeline, run_analyze, run_lineage
from src.models.graph import CartographyResult

app = typer.Typer(
    name="cartographer",
    help="Brownfield Cartographer — Codebase Intelligence System",
    add_completion=False,
)
console = Console()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _resolve_repo(repo: str, clone_dir: Path = Path("_repos")) -> Path:
    """
    Accept a local path or a GitHub URL.
    If URL, clone into clone_dir and return the local path.
    """
    if repo.startswith("http://") or repo.startswith("https://") or repo.startswith("git@"):
        repo_name = repo.rstrip("/").split("/")[-1].removesuffix(".git")
        target = clone_dir / repo_name
        if target.exists():
            logger.info(f"Repo already cloned at {target} — using existing clone.")
        else:
            console.print(f"[cyan]Cloning[/cyan] {repo} → {target}")
            clone_dir.mkdir(parents=True, exist_ok=True)
            subprocess.check_call(["git", "clone", "--depth=50", repo, str(target)])
        return target
    return Path(repo).resolve()


# ─── analyze subcommand ───────────────────────────────────────────────────────

@app.command()
def analyze(
    repo: str = typer.Argument(".", help="Local path or GitHub URL to analyse"),
    output: Path = typer.Option(Path(".cartography"), "--output", "-o", help="Where to save artifacts"),
    no_llm: bool = typer.Option(False, "--no-llm", help="Skip semantic enrichment to save LLM budget"),
):
    """
    Analyze a repository and produce .cartography/ artifacts.
    """
    console.print(Panel.fit(
        "[bold cyan]Brownfield Cartographer[/bold cyan]\n"
        "Building your codebase intelligence map...",
        border_style="cyan",
    ))

    repo_path = _resolve_repo(repo)

    if not repo_path.exists():
        console.print(f"[red]Error:[/red] Repository path not found: {repo_path}")
        raise typer.Exit(code=1)

    console.print(f"[green]Target:[/green] {repo_path}")
    console.print(f"[green]Output:[/green] {output}")
    if no_llm:
        console.print("[yellow]Phase 3 (Semanticist) will be skipped.[/yellow]")
    console.print()

    run_full_pipeline(
        repo_path=repo_path,
        output_dir=output if output.is_absolute() else repo_path / output,
        skip_semantics=no_llm,
    )

    console.print()
    console.print(Panel.fit(
        f"[bold green]Analysis complete![/bold green]\n"
        f"  Artifacts written: {repo_path / '.cartography'}",
        border_style="green",
    ))


# ─── query subcommand ─────────────────────────────────────────────────────────

@app.command()
def query(
    repo: str = typer.Argument(..., help="Local path to an already-analysed repository"),
    tool: str = typer.Option(
        "explain_module", "--tool", "-t",
        help="Tool to invoke: find_implementation | trace_lineage | blast_radius | explain_module",
    ),
    arg: str = typer.Option(..., "--arg", "-a", help="Argument to pass to the tool"),
    direction: str = typer.Option(
        "upstream", "--direction", "-d",
        help="For trace_lineage: upstream | downstream",
    ),
    cartography_dir: Optional[Path] = typer.Option(
        None, "--cartography-dir",
        help="Path to .cartography/ dir (default: <repo>/.cartography)",
    ),
):
    """
    Query the codebase knowledge graph using the Navigator agent.

    Examples:
        cartographer query ./repo --tool blast_radius --arg src/transforms/revenue.py
        cartographer query ./repo --tool trace_lineage --arg orders --direction upstream
        cartographer query ./repo --tool find_implementation --arg "revenue calculation"
        cartographer query ./repo --tool explain_module --arg src/models/orders.py
    """
    import json

    repo_path = Path(repo).resolve()
    cart_dir = cartography_dir or (repo_path / ".cartography")

    # Load persisted graphs
    module_graph_path = cart_dir / "module_graph.json"
    lineage_graph_path = cart_dir / "lineage_graph.json"

    if not module_graph_path.exists():
        console.print(
            f"[red]Error:[/red] No analysis found at {cart_dir}. "
            "Run `cartographer analyze` first."
        )
        raise typer.Exit(code=1)

    from src.models.graph import ModuleGraph, DataLineageGraph, CartographyResult
    from src.agents.navigator import Navigator

    module_graph = ModuleGraph.model_validate_json(module_graph_path.read_text())
    lineage_graph = DataLineageGraph.model_validate_json(lineage_graph_path.read_text())

    result = CartographyResult(
        repo_path=str(repo_path),
        repo_name=repo_path.name,
        analysis_timestamp="loaded-from-disk",
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    nav = Navigator(result)

    console.print(f"[cyan]Tool:[/cyan] {tool}  [cyan]Arg:[/cyan] {arg}\n")

    if tool == "find_implementation":
        output = nav.find_implementation(arg)
    elif tool == "trace_lineage":
        output = nav.trace_lineage(arg, direction=direction)  # type: ignore[arg-type]
    elif tool == "blast_radius":
        output = nav.blast_radius(arg)
    elif tool == "explain_module":
        output = nav.explain_module(arg)
    else:
        console.print(f"[red]Unknown tool:[/red] {tool}")
        raise typer.Exit(code=1)

    console.print(output)


# ─── ask subcommand ───────────────────────────────────────────────────────────

@app.command()
def ask(
    repo: str = typer.Argument(..., help="Local path to an already-analysed repository"),
    question: str = typer.Argument(..., help="Natural language question for the Navigator agent"),
    cartography_dir: Optional[Path] = typer.Option(
        None, "--cartography-dir",
        help="Path to .cartography/ dir (default: <repo>/.cartography)",
    ),
):
    """
    Ask the LangGraph Navigator agent a free-form query. It will intelligently use
    available tools to find the answer and cite its sources.
    
    Example:
        cartographer ask ./repo "What is the blast radius if the Kafka consumer fails?"
    """
    import json
    repo_path = Path(repo).resolve()
    cart_dir = cartography_dir or (repo_path / ".cartography")

    module_graph_path = cart_dir / "module_graph.json"
    lineage_graph_path = cart_dir / "lineage_graph.json"

    if not module_graph_path.exists():
        console.print(f"[red]Error:[/red] No analysis found at {cart_dir}. Run `cartographer analyze` first.")
        raise typer.Exit(code=1)

    from src.models.graph import ModuleGraph, DataLineageGraph, CartographyResult
    from src.agents.navigator import Navigator

    module_graph = ModuleGraph.model_validate_json(module_graph_path.read_text())
    lineage_graph = DataLineageGraph.model_validate_json(lineage_graph_path.read_text())

    result = CartographyResult(
        repo_path=str(repo_path),
        repo_name=repo_path.name,
        analysis_timestamp="loaded-from-disk",
        module_graph=module_graph,
        lineage_graph=lineage_graph,
    )

    nav = Navigator(result)

    console.print(f"[bold cyan]Question:[/bold cyan] {question}\n")
    with console.status("[cyan]Navigator is exploring the codebase graph...[/cyan]", spinner="dots"):
        answer = nav.ask(question)
    
    console.print(Panel.fit(
        answer,
        title="Navigator Response",
        border_style="green"
    ))

# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app()
