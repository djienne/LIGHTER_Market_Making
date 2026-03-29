#!/usr/bin/env python3
"""Deploy lighter_MM to remote VPS.

Usage:
    python deploy.py                  # zip + upload to default VPS
    python deploy.py --host 1.2.3.4   # custom host
    python deploy.py --zip-only       # just create the zip, don't upload
"""

import argparse
import os
import subprocess
import sys
import zipfile
from pathlib import Path

# Defaults — override via env vars or CLI args (no secrets in code)
DEFAULT_HOST = os.environ.get("DEPLOY_HOST", "")
DEFAULT_USER = os.environ.get("DEPLOY_USER", "ubuntu")
DEFAULT_KEY = os.environ.get("DEPLOY_KEY", "lighter.pem")
DEFAULT_DEST = os.environ.get("DEPLOY_DEST", "/home/ubuntu/")
ZIP_NAME = "lighter_MM.zip"

# Patterns to exclude from the zip
EXCLUDE_PATTERNS = {
    # Directories
    "OLD/", ".git/", "__pycache__/", "venv/", ".claude/", "logs/",
    "build/", "dist/", "*.egg-info/", "lighter_data/", "data/",
    ".pytest_cache/", "tests_live/",
    # Files
    "*.pem", "*.so", ".env", ".coverage", "*.pyc", "*.pyo",
    "*.parquet", "*.nbc", "*.nbi", "*.zip", "*.c",
    # Deploy artifacts
    ZIP_NAME,
}


def should_exclude(path: str) -> bool:
    """Check if a file path matches any exclusion pattern."""
    parts = Path(path).parts
    for pattern in EXCLUDE_PATTERNS:
        if pattern.endswith("/"):
            # Directory pattern
            dirname = pattern.rstrip("/")
            if dirname in parts:
                return True
        elif pattern.startswith("*."):
            # Extension pattern
            ext = pattern[1:]  # e.g. ".pem"
            if path.endswith(ext):
                return True
        else:
            # Exact filename
            if os.path.basename(path) == pattern:
                return True
    return False


def create_zip(project_dir: str, zip_path: str) -> int:
    """Create a zip of the project, excluding unwanted files. Returns file count."""
    count = 0
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(project_dir):
            # Prune excluded directories in-place
            dirs[:] = [
                d for d in dirs
                if not should_exclude(os.path.relpath(os.path.join(root, d), project_dir) + "/")
            ]
            for fname in sorted(files):
                full = os.path.join(root, fname)
                rel = os.path.relpath(full, project_dir)
                if should_exclude(rel):
                    continue
                zf.write(full, os.path.join("lighter_MM", rel))
                count += 1
    return count


def upload(zip_path: str, host: str, user: str, key: str, dest: str) -> bool:
    """Upload zip to remote VPS via scp. Returns True on success."""
    key_path = key if os.path.exists(key) else os.path.expanduser(f"~/.ssh/{key}")
    if not os.path.exists(key_path):
        print(f"ERROR: SSH key not found: {key} or ~/.ssh/{key}")
        return False

    cmd = [
        "scp",
        "-i", key_path,
        "-o", "StrictHostKeyChecking=no",
        zip_path,
        f"{user}@{host}:{dest}",
    ]
    print(f"Uploading {zip_path} -> {user}@{host}:{dest}")
    result = subprocess.run(cmd)
    return result.returncode == 0


def remote_unzip(host: str, user: str, key: str, dest: str, zip_name: str) -> bool:
    """Unzip on the remote VPS. Returns True on success."""
    key_path = key if os.path.exists(key) else os.path.expanduser(f"~/.ssh/{key}")
    cmd = [
        "ssh",
        "-i", key_path,
        "-o", "StrictHostKeyChecking=no",
        f"{user}@{host}",
        f"cd {dest} && unzip -o {zip_name}",
    ]
    print(f"Unzipping on {host}...")
    result = subprocess.run(cmd)
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description="Deploy lighter_MM to remote VPS")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"VPS hostname/IP (default: {DEFAULT_HOST})")
    parser.add_argument("--user", default=DEFAULT_USER, help=f"SSH user (default: {DEFAULT_USER})")
    parser.add_argument("--key", default=DEFAULT_KEY, help=f"SSH key file (default: {DEFAULT_KEY})")
    parser.add_argument("--dest", default=DEFAULT_DEST, help=f"Remote destination (default: {DEFAULT_DEST})")
    parser.add_argument("--zip-only", action="store_true", help="Only create the zip, don't upload")
    parser.add_argument("--no-unzip", action="store_true", help="Upload but don't unzip on remote")
    args = parser.parse_args()

    if not args.zip_only and not args.host:
        print("ERROR: No host specified. Use --host or set DEPLOY_HOST env var.")
        sys.exit(1)

    project_dir = os.path.dirname(os.path.abspath(__file__))
    zip_path = os.path.join(project_dir, ZIP_NAME)

    # Create zip
    print(f"Creating {ZIP_NAME}...")
    count = create_zip(project_dir, zip_path)
    size_mb = os.path.getsize(zip_path) / (1024 * 1024)
    print(f"  {count} files, {size_mb:.1f} MB")

    if args.zip_only:
        print(f"Zip created: {zip_path}")
        return

    # Upload
    if not upload(zip_path, args.host, args.user, args.key, args.dest):
        print("ERROR: Upload failed")
        sys.exit(1)
    print("Upload OK")

    # Unzip on remote
    if not args.no_unzip:
        if not remote_unzip(args.host, args.user, args.key, args.dest, ZIP_NAME):
            print("WARNING: Remote unzip failed (you may need to install unzip on the VPS)")
        else:
            print("Unzip OK")

    print(f"\nDeployed to {args.user}@{args.host}:{args.dest}lighter_MM/")
    print("Next steps on VPS:")
    print(f"  cd {args.dest}lighter_MM")
    print("  pip install -r requirements.txt  # if needed")
    print("  python setup_cython.py build_ext --inplace")
    print("  cp /path/to/.env .env")
    print("  python -u market_maker_v2.py --symbol BTC --grid grid_config.json")


if __name__ == "__main__":
    main()
