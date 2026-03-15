from __future__ import annotations
import hashlib
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Generator, Any

from loguru import logger
from tree_sitter import Node, Parser, Language, Tree, Query, QueryCursor

from src.models.nodes import ModuleNode


@dataclass
class ParseResult:
    """Unified result of a tree-sitter parse operation."""
    root_node: Node
    source_bytes: bytes
    language: str
    file_path: Path
    tree: Tree


class LanguageRouter:
    """Routes file paths to language identifiers."""

    def get_language_for_path(self, path: Path) -> str:
        """
        Returns the language identifier for a given path.
        """
        ext = path.suffix.lower()
        mapping = {
            ".py": "python",
            ".sql": "sql",
            ".yaml": "yaml",
            ".yml": "yaml",
            ".js": "javascript",
            ".mjs": "javascript",
            ".cjs": "javascript",
            ".ts": "typescript",
            ".tsx": "typescript",
            ".ipynb": "notebook",
        }
        lang = mapping.get(ext, "unknown")
        if lang == "unknown" and path.is_file():
            # Try shebang detection
            try:
                with open(path, "rb") as f:
                    first_line = f.readline().decode("utf-8", errors="replace")
                    if first_line.startswith("#!"):
                        if "python" in first_line: return "python"
                        if "bash" in first_line or "sh " in first_line: return "bash"
                        if "node" in first_line: return "javascript"
            except Exception:
                pass
        return lang


class TreeSitterAnalyzer:
    """
    Enhanced foundation for AST parsing using tree-sitter.
    Features:
    - Class-level caching for parsers, languages, and ASTs.
    - Thread-safe access.
    - 1MB file size limit.
    - AST traversal and query helpers.
    """

    # Class-level caches and locks
    _parsers: Dict[str, Parser] = {}
    _languages: Dict[str, Any] = {}
    _parse_cache: Dict[str, ParseResult] = {}
    _query_cache: Dict[str, Query] = {}
    _lock = threading.Lock()
    
    MAX_FILE_SIZE = 1 * 1024 * 1024  # 1MB
    CACHE_SIZE_LIMIT = 500  # Simple limit for AST cache

    def __init__(self) -> None:
        self.router = LanguageRouter()

    def _get_parser(self, lang_name: str) -> Optional[Parser]:
        """Thread-safe acquisition of a parser."""
        with self._lock:
            if lang_name in self._parsers:
                return self._parsers[lang_name]

            try:
                lang_capsule = None
                if lang_name == "python":
                    import tree_sitter_python
                    lang_capsule = tree_sitter_python.language()
                elif lang_name == "javascript":
                    import tree_sitter_javascript
                    lang_capsule = tree_sitter_javascript.language()
                elif lang_name == "typescript":
                    import tree_sitter_typescript
                    # Typescript has two languages: typescript and tsx
                    # We'll use the basic one for now or detect via extension later if needed
                    lang_capsule = tree_sitter_typescript.language_typescript()
                elif lang_name == "yaml":
                    import tree_sitter_yaml
                    lang_capsule = tree_sitter_yaml.language()
                elif lang_name == "sql":
                    # Placeholder for future sql parser integration
                    return None
                else:
                    return None

                if lang_capsule:
                    lang = Language(lang_capsule)
                    parser = Parser()
                    parser.language = lang
                    self._parsers[lang_name] = parser
                    return parser
            except (ImportError, Exception) as exc:
                logger.warning(f"Failed to initialize tree-sitter parser for {lang_name}: {exc}")
            return None

    def parse_file(self, path: Path) -> Optional[ParseResult]:
        """
        Checks cache, then parses a file. Returns ParseResult or None.
        """
        if not path.is_file():
            return None

        # 1. Size Check
        try:
            if path.stat().st_size > self.MAX_FILE_SIZE:
                logger.warning(f"Skipping large file for AST parsing: {path}")
                return None
        except Exception:
            return None

        # 2. Language Detection
        lang_name = self.router.get_language_for_path(path)
        if lang_name == "unknown":
            return None

        # 3. Read & Hash for Caching
        try:
            source_bytes = path.read_bytes()
            
            # Special handling for Notebooks: extract code cells
            if lang_name == "notebook":
                source_bytes = self.extract_notebook_code(source_bytes)
                lang_name = "python" # Analyze as Python

            content_hash = hashlib.md5(source_bytes).hexdigest()
            cache_key = f"{path}:{content_hash}"
            
            with self._lock:
                if cache_key in self._parse_cache:
                    return self._parse_cache[cache_key]
        except Exception as exc:
            logger.error(f"Error reading/preparing file {path}: {exc}")
            return None

        # 4. Parse
        parser = self._get_parser(lang_name)
        if not parser:
            return None

        try:
            tree = parser.parse(source_bytes)
            result = ParseResult(
                root_node=tree.root_node,
                source_bytes=source_bytes,
                language=lang_name,
                file_path=path,
                tree=tree
            )
            
            # Simple cache management
            with self._lock:
                if len(self._parse_cache) >= self.CACHE_SIZE_LIMIT:
                    # Clear half if full
                    self._parse_cache.clear()
                self._parse_cache[cache_key] = result
                
            return result
        except Exception as exc:
            logger.error(f"Tree-sitter parsing failed for {path}: {exc}")
            return None

    # ─── UTILITIES ──────────────────────────────────────────────────

    @staticmethod
    def walk_tree(root: Node) -> Generator[Node, None, None]:
        """Depth-first traversal generator for AST nodes."""
        stack = [root]
        while stack:
            node = stack.pop()
            yield node
            # Add in reverse to process left-to-right
            for i in range(len(node.children) - 1, -1, -1):
                stack.append(node.children[i])

    @staticmethod
    def get_node_text(node: Node, source_bytes: bytes) -> str:
        """Extract and decode text for a specific AST node."""
        return source_bytes[node.start_byte:node.end_byte].decode("utf-8", errors="replace")

    def run_query(self, language_name: str, root_node: Node, query_string: str) -> List[Dict[str, Node]]:
        """
        Execute a tree-sitter query and return matches using the new API.
        """
        parser = self._get_parser(language_name)
        if not parser or not parser.language:
            return []
            
        try:
            # Check cache first
            cache_key = f"{language_name}:{query_string}"
            with self._lock:
                if cache_key in self._query_cache:
                    query = self._query_cache[cache_key]
                else:
                    # New API: Query(language, query_string)
                    query = Query(parser.language, query_string)
                    self._query_cache[cache_key] = query

            # New API: QueryCursor(query)
            cursor = QueryCursor(query)
            
            # New API: cursor.captures(node)
            captures_raw = cursor.captures(root_node)
            
            results = []
            if isinstance(captures_raw, dict):
                # Flatten the dictionary of lists into a list of single-key dicts
                for name, nodes in captures_raw.items():
                    for node in nodes:
                        results.append({name: node})
            else:
                # Handle list of (node, name) tuples (old API or iterator behavior)
                for node, name in captures_raw:
                    results.append({name: node})
            return results
        except Exception as exc:
            logger.error(f"Query execution failed for {language_name}: {exc}")
            return []

    def extract_python_imports(self, root: Node, source_bytes: bytes) -> list[str]:
        """
        Extract and normalize Python imports from the AST.
        """
        imports = []

        def _get_text(node: Node) -> str:
            return self.get_node_text(node, source_bytes)

        # We can use walk_tree for this too now
        for node in self.walk_tree(root):
            try:
                if node.type == "import_statement":
                    for child in node.children:
                        if child.type == "dotted_name":
                            imports.append(_get_text(child))
                        elif child.type == "aliased_import":
                            name_node = child.child_by_field_name("name")
                            if name_node:
                                imports.append(_get_text(name_node))
                                
                elif node.type == "import_from_statement":
                    module_name = ""
                    for child in node.children:
                        if child.type == "import":
                            break
                        if child.type in ("dotted_name", "relative_import"):
                            module_name = _get_text(child)
                    
                    found_import = False
                    for child in node.children:
                        if child.type == "import":
                            found_import = True
                            continue
                        if not found_import:
                            continue
                            
                        symbol_nodes = []
                        if child.type == "import_list":
                            symbol_nodes = [cn for cn in child.children if cn.type in ("dotted_name", "aliased_import", "wildcard_import")]
                        elif child.type in ("dotted_name", "aliased_import", "wildcard_import"):
                            symbol_nodes = [child]
                            
                        for sn in symbol_nodes:
                            raw_name = ""
                            if sn.type == "dotted_name":
                                raw_name = _get_text(sn)
                            elif sn.type == "aliased_import":
                                aliased_name_node = sn.child_by_field_name("name")
                                if aliased_name_node:
                                    raw_name = _get_text(aliased_name_node)
                            elif sn.type == "wildcard_import":
                                raw_name = "*"
                                
                            if raw_name:
                                if module_name:
                                    if module_name.endswith(".") or raw_name.startswith("."):
                                        imports.append(f"{module_name}{raw_name}")
                                    else:
                                        imports.append(f"{module_name}.{raw_name}")
                                else:
                                    imports.append(raw_name)
            except Exception as exc:
                logger.debug(f"Error extracting import node: {exc}")
                continue
        
        return sorted(list(set(imports)))

    def resolve_import_path(self, raw_import: str, current_file: Path, repo_root: Path) -> Optional[str]:
        """
        Resolves a dotted import or relative import to a physical file path
        relative to the repository root.
        """
        try:
            # 1. Handle Relative Imports
            if raw_import.startswith("."):
                dots = 0
                for char in raw_import:
                    if char == ".":
                        dots += 1
                    else:
                        break
                
                # . -> same dir, .. -> parent, etc.
                # current_file.parent is where we start
                base_dir = current_file.parent
                for _ in range(dots - 1):
                    base_dir = base_dir.parent
                
                module_tail = raw_import[dots:].replace(".", "/")
                if not module_tail: # e.g. "from .. import x"
                    # We might be importing the package itself or something from __init__
                    # But usually there's a symbol. If it's just dots, it's the directory
                    potential_path = base_dir
                else:
                    potential_path = base_dir / module_tail
            else:
                # 2. Handle Absolute Imports (from repo root)
                potential_path = repo_root / raw_import.replace(".", "/")

            # 3. Check for .py or __init__.py
            # Try as file.py
            file_py = potential_path.with_suffix(".py")
            if file_py.is_file():
                return str(file_py.relative_to(repo_root)).replace("\\", "/")
            
            # Try as dir/__init__.py
            init_py = potential_path / "__init__.py"
            if init_py.is_file():
                return str(init_py.relative_to(repo_root)).replace("\\", "/")
            
            if potential_path.is_dir():
                return str(potential_path.relative_to(repo_root)).replace("\\", "/")

            # 4. Handle symbols (e.g. from module.sub import function)
            # If we didn't find the file/dir, maybe the last part is a symbol?
            if "." in raw_import:
                parent_import = ".".join(raw_import.split(".")[:-1])
                # Prevents infinite recursion if we already tried this
                if parent_import != raw_import:
                    return self.resolve_import_path(parent_import, current_file, repo_root)

        except Exception as exc:
            logger.debug(f"Failed to resolve import {raw_import}: {exc}")
        
        return None

    def extract_python_functions(self, root: Node, source_bytes: bytes) -> list[str]:
        """
        Extract all function names recursively, formatting methods as Class.method.
        """
        functions = []
        
        # Use a list to simulate a stack to track class nesting
        # (node, current_scope_name)
        stack = [(root, "")]
        
        while stack:
            node, scope = stack.pop()
            
            if node.type == "function_definition":
                name_node = node.child_by_field_name("name")
                if name_node:
                    raw_name = self.get_node_text(name_node, source_bytes)
                    name = raw_name.lstrip("_")
                    if scope:
                        functions.append(f"{scope}.{name}")
                    else:
                        functions.append(name)
                # We don't stop walking; we might have functions inside functions
                # but we keep the current scope for nested functions unless we want something like func.nested
                # Usually we just want the top-level or method scope.
                # Let's keep the scope for inner functions too? 
                # Instructions say: "ClassName.method_name".
                # For inner functions, let's keep it simple for now and just use the function's own name or func.inner
                # If it's already in a scope (like a method), we could do Class.method.inner
                # Let's just pass the same scope for now or extend it.
                # For now, let's just use the current scope.
                
            elif node.type == "class_definition":
                name_node = node.child_by_field_name("name")
                if name_node:
                    class_name = self.get_node_text(name_node, source_bytes)
                    new_scope = f"{scope}.{class_name}" if scope else class_name
                    # Nested class support: Outer.Inner
                    for child in reversed(node.children):
                        stack.append((child, new_scope))
                    continue # Skip the default child addition loop
                    
            elif node.type == "decorated_definition":
                # Find the actual definition inside
                for child in reversed(node.children):
                    stack.append((child, scope))
                continue

            # Default: add children to stack
            for child in reversed(node.children):
                stack.append((child, scope))
                
        return sorted(list(set(functions)))

    def extract_python_classes(self, root: Node, source_bytes: bytes) -> tuple[list[str], dict[str, list[str]]]:
        """Extract all classes recursively, supporting Outer.Inner."""
        classes = []
        bases_map = {}
        
        stack = [(root, "")]
        while stack:
            node, scope = stack.pop()
            
            if node.type == "class_definition":
                name_node = node.child_by_field_name("name")
                if name_node:
                    raw_name = self.get_node_text(name_node, source_bytes)
                    full_name = f"{scope}.{raw_name}" if scope else raw_name
                    classes.append(full_name)
                    
                    bases = []
                    arg_list = node.child_by_field_name("superclasses")
                    if arg_list:
                        for arg in arg_list.children:
                            if arg.type not in ("(", ")", ","):
                                bases.append(self.get_node_text(arg, source_bytes))
                    bases_map[full_name] = bases
                    
                    # Nesting support
                    for child in reversed(node.children):
                        stack.append((child, full_name))
                    continue

            for child in reversed(node.children):
                stack.append((child, scope))
                
        return sorted(classes), bases_map

    def extract_notebook_code(self, source_bytes: bytes) -> bytes:
        """Extracts code cells from a .ipynb JSON buffer and joins them."""
        try:
            import json
            data = json.loads(source_bytes.decode("utf-8"))
            code_cells = []
            for cell in data.get("cells", []):
                if cell.get("cell_type") == "code":
                    source = cell.get("source", [])
                    if isinstance(source, list):
                        code_cells.append("".join(source))
                    else:
                        code_cells.append(source)
            return "\n".join(code_cells).encode("utf-8")
        except Exception as exc:
            logger.warning(f"Failed to extract notebook code: {exc}")
            return b""

    def compute_complexity(self, node: Node) -> int:
        """
        Computes cyclomatic complexity for a given node (usually a function).
        CC = E - N + 2P, but we use the simpler: 1 + number of decision points.
        """
        # Decision points in Python: if, elif, while, for, except, with, and, or
        decision_points = {
            "if_statement", "elif_clause", "while_statement", "for_statement",
            "except_clause", "with_statement", "boolean_operator"
        }
        
        count = 0
        for n in self.walk_tree(node):
            if n.type in decision_points:
                count += 1
            # Special case for boolean_operator: count its children that are actually operators (and/or)
            elif n.type == "boolean_operator":
                # Tree-sitter boolean_operator contains 'and' / 'or' as children
                pass # Already counted via type matching in some grammars, but let's be safe
        
        return 1 + count

    def compute_code_metrics(self, source_bytes: bytes) -> dict:
        """
        Computes line counts and comment ratio.
        """
        try:
            lines = source_bytes.decode("utf-8").splitlines()
            total_lines = len(lines)
            if total_lines == 0:
                return {"loc": 0, "comment_ratio": 0.0}
            
            comment_lines = 0
            for line in lines:
                trimmed = line.strip()
                if trimmed.startswith("#") or trimmed.startswith("//") or trimmed.startswith("--"):
                    comment_lines += 1
                elif '"""' in trimmed or "'''" in trimmed:
                    # Very simple multi-line comment approximation
                    comment_lines += 1
            
            return {
                "loc": total_lines,
                "comment_ratio": round(comment_lines / total_lines, 2)
            }
        except Exception:
            return {"loc": 0, "comment_ratio": 0.0}


def analyze_module(path: Path, repo_root: Path) -> Optional[ModuleNode]:
    """
    Main entry point for module analysis.
    Now includes complexity, LOC, and comment ratio.
    """
    from src.models.nodes import Language
    analyzer = TreeSitterAnalyzer()
    
    rel_path = str(path.relative_to(repo_root)).replace("\\", "/")
    lang_name = analyzer.router.get_language_for_path(path)
    
    # Map string to Language enum
    lang_enum = Language.UNKNOWN
    if lang_name == "python" or lang_name == "notebook": lang_enum = Language.PYTHON
    elif lang_name == "sql": lang_enum = Language.SQL
    elif lang_name == "yaml": lang_enum = Language.YAML
    elif lang_name == "javascript": lang_enum = Language.JAVASCRIPT
    elif lang_name == "typescript": lang_enum = Language.TYPESCRIPT

    node = ModuleNode(path=rel_path, language=lang_enum)
    node.analysis_method = "tree_sitter"
    
    try:
        # Size Check
        if path.stat().st_size > analyzer.MAX_FILE_SIZE:
            logger.warning(f"Skipping large module: {rel_path} ({path.stat().st_size} bytes)")
            node.confidence = 0.0
            return node

        logger.debug(f"Analyzing module: {rel_path}")
        
        # Basic Metrics (LOC, Comment Ratio)
        source_bytes = path.read_bytes()
        if lang_name == "notebook":
            source_bytes = analyzer.extract_notebook_code(source_bytes)
        
        metrics = analyzer.compute_code_metrics(source_bytes)
        node.lines_of_code = metrics["loc"]
        node.comment_ratio = metrics["comment_ratio"]
        
        if lang_name in ("python", "notebook"):
            result = analyzer.parse_file(path)
            if result:
                # Imports
                raw_imports = analyzer.extract_python_imports(result.root_node, result.source_bytes)
                resolved_imports = set()
                for imp in raw_imports:
                    resolved = analyzer.resolve_import_path(imp, path, repo_root)
                    if resolved:
                        resolved_imports.add(resolved)
                node.imports = sorted(list(resolved_imports))
                
                # Functions and Classes
                node.public_functions = analyzer.extract_python_functions(result.root_node, result.source_bytes)
                node.classes, node.bases = analyzer.extract_python_classes(result.root_node, result.source_bytes)
                
                # Complexity (average across functions)
                func_complexities = []
                # Simple query to find all functions
                query = "(function_definition) @func"
                matches = analyzer.run_query("python", result.root_node, query)
                for m in matches:
                    for n in m.values():
                        func_complexities.append(analyzer.compute_complexity(n))
                
                if func_complexities:
                    node.complexity_score = round(sum(func_complexities) / len(func_complexities), 2)
                    node.max_complexity = float(max(func_complexities))
                else:
                    node.complexity_score = 1.0 # Minimum CC
                    node.max_complexity = 1.0
                    
                node.confidence = 1.0
        elif lang_name == "sql":
            from src.analyzers.sql_lineage import SQLLineageAnalyzer
            analyzer_sql = SQLLineageAnalyzer(repo_root=repo_root)
            transformation = analyzer_sql.analyze(path)
            if transformation:
                node.imports = sorted(list(set(transformation.source_datasets + transformation.target_datasets)))
                node.analysis_method = "sqlglot"
                node.confidence = 0.9
            else:
                node.analysis_method = "heuristic"
                node.confidence = 0.1
    except Exception as exc:
        logger.error(f"Error analyzing module {rel_path}: {exc}")
        node.confidence = 0.5
        node.parsing_error = str(exc)
    
    return node
