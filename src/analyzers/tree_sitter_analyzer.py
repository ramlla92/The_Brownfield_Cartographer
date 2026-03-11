from __future__ import annotations
from pathlib import Path
from loguru import logger
from tree_sitter import Node, Parser, Language
from typing import Optional, Dict

from src.models.nodes import ModuleNode


class LanguageRouter:
    """Routes file paths to language identifiers."""

    def get_language_for_path(self, path: Path) -> str:
        """
        Returns the language identifier for a given path.
        
        Supported: python, sql, yaml, javascript, typescript.
        """
        ext = path.suffix.lower()
        if ext == ".py":
            return "python"
        elif ext == ".sql":
            return "sql"
        elif ext in (".yaml", ".yml"):
            return "yaml"
        elif ext == ".js":
            return "javascript"
        elif ext in (".ts", ".tsx"):
            return "typescript"
        return "unknown"


class TreeSitterAnalyzer:
    """
    Foundation for AST parsing using tree-sitter.
    Lazily initializes parsers for supported languages.
    """

    def __init__(self) -> None:
        self.router = LanguageRouter()
        self._parsers: Dict[str, Parser] = {}

    def _initialize_parser(self, lang_name: str) -> Optional[Parser]:
        """Lazily initialize a tree-sitter parser for the given language."""
        if lang_name in self._parsers:
            return self._parsers[lang_name]

        try:
            lang_capsule = None
            if lang_name == "python":
                import tree_sitter_python as tspython
                lang_capsule = tspython.language()
            elif lang_name == "javascript":
                import tree_sitter_javascript as tsjs
                lang_capsule = tsjs.language()
            elif lang_name == "yaml":
                import tree_sitter_yaml as tsyaml
                lang_capsule = tsyaml.language()
            elif lang_name == "typescript":
                # TODO: add tree-sitter-typescript dependency
                return None
            elif lang_name == "sql":
                # TODO: add tree-sitter-sql dependency
                return None
            else:
                return None

            if lang_capsule:
                lang = Language(lang_capsule)
                parser = Parser()
                parser.language = lang
                self._parsers[lang_name] = parser
                return parser
            return None
        except (ImportError, AttributeError, Exception) as exc:
            logger.warning(f"Failed to initialize tree-sitter parser for {lang_name}: {exc}")
            return None

    def parse_file(self, path: Path) -> Optional[Node]:
        """
        Parses a file and returns its tree-sitter root node.
        
        Returns None if language is unknown or parser init fails.
        """
        lang_name = self.router.get_language_for_path(path)
        if lang_name == "unknown":
            return None

        parser = self._initialize_parser(lang_name)
        if not parser:
            return None

        try:
            content = path.read_text(encoding="utf-8", errors="replace")
            tree = parser.parse(content.encode("utf-8"))
            return tree.root_node
        except Exception as exc:
            logger.error(f"Error parsing file {path}: {exc}")
            return None

    def extract_python_imports(self, root: Node, source: str) -> list[str]:
        """
        Extract and normalize Python imports from the AST.
        """
        imports = []
        source_bytes = source.encode("utf-8")

        def _get_text(node: Node) -> str:
            return source_bytes[node.start_byte:node.end_byte].decode("utf-8", errors="replace")

        queue = [root]
        while queue:
            node = queue.pop(0)
            try:
                if node.type == "import_statement":
                    # import a, b.c as d
                    for child in node.children:
                        if child.type == "dotted_name":
                            imports.append(_get_text(child))
                        elif child.type == "aliased_import":
                            name_node = child.child_by_field_name("name")
                            if name_node:
                                imports.append(_get_text(name_node))
                                
                elif node.type == "import_from_statement":
                    # from x import y, z
                    module_name = ""
                    # The module is the dotted_name or relative_import before 'import'
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
                            
                        # Symbols after 'import'
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
                                    # Handle relative and absolute imports
                                    if module_name.endswith(".") or raw_name.startswith("."):
                                        imports.append(f"{module_name}{raw_name}")
                                    else:
                                        imports.append(f"{module_name}.{raw_name}")
                                else:
                                    imports.append(raw_name)
                else:
                    queue.extend(node.children)
            except Exception as exc:
                logger.debug(f"Error extracting import node: {exc}")
                continue
        
        return sorted(list(set(imports)))

    def extract_python_functions(self, root: Node, source: str) -> list[str]:
        """Extract top-level function names, stripping leading underscores."""
        functions = []
        source_bytes = source.encode("utf-8")

        def _process_node(node: Node):
            if node.type == "function_definition":
                name_node = node.child_by_field_name("name")
                if name_node:
                    name = source_bytes[name_node.start_byte:name_node.end_byte].decode("utf-8")
                    functions.append(name.lstrip("_"))
                return True # Handled
            elif node.type == "decorated_definition":
                for child in node.children:
                    if _process_node(child):
                        return True
            return False

        for child in root.children:
            try:
                _process_node(child)
            except Exception as exc:
                logger.debug(f"Error extracting function node: {exc}")
                continue
        return sorted(list(set(functions)))

    def extract_python_classes(self, root: Node, source: str) -> tuple[list[str], dict[str, list[str]]]:
        """Extract top-level classes and their bases."""
        classes = []
        bases_map = {}
        source_bytes = source.encode("utf-8")

        def _get_text(node: Node) -> str:
            return source_bytes[node.start_byte:node.end_byte].decode("utf-8", errors="replace")

        def _process_node(node: Node):
            if node.type == "class_definition":
                name_node = node.child_by_field_name("name")
                if name_node:
                    class_name = _get_text(name_node)
                    classes.append(class_name)
                    
                    bases = []
                    arg_list = node.child_by_field_name("superclasses")
                    if arg_list:
                        for arg in arg_list.children:
                            if arg.type not in ("(", ")", ","):
                                bases.append(_get_text(arg))
                    bases_map[class_name] = bases
                return True
            elif node.type == "decorated_definition":
                for child in node.children:
                    if _process_node(child):
                        return True
            return False

        for child in root.children:
            try:
                _process_node(child)
            except Exception as exc:
                logger.debug(f"Error extracting class node: {exc}")
                continue
        return sorted(classes), bases_map


def analyze_module(path: Path, repo_root: Path) -> Optional[ModuleNode]:
    """
    Main entry point for module analysis.
    """
    analyzer = TreeSitterAnalyzer()
    
    rel_path = str(path.relative_to(repo_root))
    lang = analyzer.router.get_language_for_path(path)
    
    node = ModuleNode(path=rel_path, language=lang)
    
    if lang == "python":
        try:
            content = path.read_text(encoding="utf-8", errors="replace")
            root_node = analyzer.parse_file(path)
            if root_node:
                node.imports = analyzer.extract_python_imports(root_node, content)
                node.public_functions = analyzer.extract_python_functions(root_node, content)
                node.classes, node.bases = analyzer.extract_python_classes(root_node, content)
        except Exception as exc:
            logger.error(f"Error analyzing Python module {rel_path}: {exc}")
    
    return node
