#!/usr/bin/env python3
"""Archive relevant project files into a timestamped zip."""

import os
import zipfile
from datetime import datetime
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent

INCLUDE_EXTENSIONS = {
    # Code
    ".py", ".rs", ".toml",
    # Config / data
    ".json", ".yml", ".yaml", ".cfg", ".ini",
    # Docs / text
    ".md", ".txt", ".rst",
    # Docker / shell
    ".sh",
}

INCLUDE_EXACT = {
    "Dockerfile",
    "requirements.txt",
    "docker-compose.yml",
    ".gitignore",
}

EXCLUDE_DIRS = {
    "venv", ".venv", "__pycache__", ".git", "node_modules",
    ".pytest_cache", "lighter_data", "logs", ".claude",
}

EXCLUDE_FILES = {
    ".env", "lighter.pem",
}


def should_include(path: Path) -> bool:
    rel = path.relative_to(PROJECT_DIR)
    # Skip excluded directories
    if any(part in EXCLUDE_DIRS for part in rel.parts):
        return False
    # Skip excluded files
    if path.name in EXCLUDE_FILES:
        return False
    # Include by exact name or extension
    return path.name in INCLUDE_EXACT or path.suffix.lower() in INCLUDE_EXTENSIONS


def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_name = f"lighter_mm_archive_{timestamp}.zip"
    zip_path = PROJECT_DIR / zip_name

    count = 0
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(PROJECT_DIR):
            # Prune excluded dirs in-place so os.walk skips them
            dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
            for fname in sorted(files):
                fpath = Path(root) / fname
                if should_include(fpath):
                    arcname = fpath.relative_to(PROJECT_DIR)
                    zf.write(fpath, arcname)
                    count += 1

    size_kb = zip_path.stat().st_size / 1024
    print(f"Created {zip_name}  ({count} files, {size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
