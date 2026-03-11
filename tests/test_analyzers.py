import pytest
from pathlib import Path
from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer, analyze_module
from src.analyzers.git_velocity import extract_git_velocity

def test_language_router():
    from src.analyzers.tree_sitter_analyzer import LanguageRouter
    router = LanguageRouter()
    assert router.get_language_for_path(Path("test.py")) == "python"
    assert router.get_language_for_path(Path("test.js")) == "javascript"
    assert router.get_language_for_path(Path("test.sql")) == "sql"
    assert router.get_language_for_path(Path("test.unknown")) == "unknown"

def test_tree_sitter_analyzer_python(tmp_path):
    # Create a dummy python file
    d = tmp_path / "subdir"
    d.mkdir()
    p = d / "hello.py"
    p.write_text("import os\n\ndef my_func():\n    pass\n\nclass MyClass(Base):\n    pass", encoding="utf-8")
    
    analyzer = TreeSitterAnalyzer()
    root_node = analyzer.parse_file(p)
    assert root_node is not None
    assert root_node.type == "module"
    
    content = p.read_text(encoding="utf-8")
    imports = analyzer.extract_python_imports(root_node, content)
    assert "os" in imports
    
    functions = analyzer.extract_python_functions(root_node, content)
    assert "my_func" in functions
    
    classes, bases = analyzer.extract_python_classes(root_node, content)
    assert "MyClass" in classes
    assert "Base" in bases["MyClass"]

def test_analyze_module_python(tmp_path):
    p = tmp_path / "logic.py"
    p.write_text("def run():\n    pass", encoding="utf-8")
    
    # repo_root should be tmp_path
    node = analyze_module(p, tmp_path)
    assert node is not None
    assert node.path == "logic.py"
    assert node.language == "python"
    assert "run" in node.public_functions

def test_git_velocity_no_git(tmp_path):
    # tmp_path has no .git dir
    velocity = extract_git_velocity(tmp_path)
    assert velocity == {}

@pytest.mark.skipif(not Path(".git").exists(), reason="Not in a git repo")
def test_git_velocity_actual():
    repo_root = Path(".").resolve()
    velocity = extract_git_velocity(repo_root)
    # The current repo should have some velocity
    assert isinstance(velocity, dict)
    if velocity:
        assert any(count > 0 for count in velocity.values())
