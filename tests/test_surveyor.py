import pytest
import json
import networkx as nx
from pathlib import Path
from src.agents.surveyor import Surveyor

def test_surveyor_full_flow(tmp_path):
    # 1. Create a mock repository
    repo = tmp_path / "mock_repo"
    repo.mkdir()
    
    # Create some files
    (repo / "cli.py").write_text("import utils\nimport models.user\n\ndef main(): pass", encoding="utf-8")
    (repo / "utils.py").write_text("def helper(): pass", encoding="utf-8")
    
    models_dir = repo / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").write_text("from .user import User", encoding="utf-8")
    (models_dir / "user.py").write_text("class User: pass", encoding="utf-8")
    
    # 2. Run Surveyor
    surveyor = Surveyor(repo)
    graph = surveyor.build_module_graph()
    
    # 3. Verify graph nodes
    assert "cli.py" in graph.nodes
    assert "utils.py" in graph.nodes
    assert "models/__init__.py" in graph.nodes
    assert "models/user.py" in graph.nodes
    
    # Verify metadata
    cli_node = graph.nodes["cli.py"]
    assert cli_node["language"] == "python"
    assert "main" in cli_node["public_functions"]
    
    # Verify imports/edges
    assert graph.has_edge("cli.py", "utils.py")
    assert graph.has_edge("models/__init__.py", "models/user.py")
    
    # Verify PageRank (just that it exists)
    assert "page_rank" in cli_node
    
    # Verify writing to JSON
    output_json = repo / "graph.json"
    surveyor.write_module_graph_json(output_json)
    assert output_json.exists()
    
    with open(output_json, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert "nodes" in data
        assert "edges" in data
        assert "cli.py" in data["nodes"]
        assert any(e["source"] == "cli.py" and e["target"] == "utils.py" for e in data["edges"])

def test_surveyor_skips(tmp_path):
    repo = tmp_path / "skip_repo"
    repo.mkdir()
    
    (repo / "valid.py").write_text("# hello", encoding="utf-8")
    
    git_dir = repo / ".git"
    git_dir.mkdir()
    (git_dir / "config").write_text("config", encoding="utf-8")
    
    venv_dir = repo / ".venv"
    venv_dir.mkdir()
    (venv_dir / "bin").mkdir()
    
    surveyor = Surveyor(repo)
    graph = surveyor.build_module_graph()
    
    assert "valid.py" in graph.nodes
    assert ".git/config" not in graph.nodes
    assert len(graph.nodes) == 1
