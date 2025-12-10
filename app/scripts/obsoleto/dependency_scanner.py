import os
import ast
import sys

PROJECT_ROOT = "./"   # o la ruta absoluta a tu proyecto

# Paquetes estándar de Python (NO se envían a requirements)
STANDARD_LIBS = {
    "os", "sys", "re", "json", "pathlib", "functools", "datetime",
    "typing", "collections", "itertools", "math", "statistics",
    "subprocess", "shutil", "hashlib", "uuid", "logging", "argparse",
    "traceback", "base64", "unittest", "tempfile", "csv", "glob",
    "threading", "asyncio", "multiprocessing", "struct"
}

found_imports = set()

def scan_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        try:
            tree = ast.parse(f.read(), filename=filepath)
        except Exception:
            return

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                pkg = alias.name.split('.')[0]
                found_imports.add(pkg)

        elif isinstance(node, ast.ImportFrom):
            if node.module:
                pkg = node.module.split('.')[0]
                found_imports.add(pkg)


def scan_project():
    for root, dirs, files in os.walk(PROJECT_ROOT):
        # Ignorar carpetas de venv, git, notebooks, etc.
        if any(substr in root for substr in ["venv", ".git", "__pycache__", "notebooks"]):
            continue

        for file in files:
            if file.endswith(".py"):
                scan_file(os.path.join(root, file))


if __name__ == "__main__":
    scan_project()

    print("=== DEPENDENCIAS DETECTADAS EN EL CÓDIGO ===")
    for pkg in sorted(found_imports):
        if pkg not in STANDARD_LIBS:
            print(pkg)
