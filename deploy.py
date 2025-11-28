"""Simple deployment helper for lighter_MM files.

Copies the specified Python files to the remote host and restarts the
Docker Compose stack.
"""

import argparse
import pathlib
import shlex
import subprocess
import sys


def run_command(command):
    """Run a command and echo it to stdout."""
    print("Executing:", " ".join(shlex.quote(arg) for arg in command))
    subprocess.check_call(command)


def build_scp_command(files, host, remote_path, identity_file=None):
    cmd = ["scp", "-r"]  # Use recursive flag for directories
    if identity_file:
        cmd.extend(["-i", identity_file])
    cmd.extend(str(path) for path in files)
    cmd.append(f"{host}:{remote_path}")
    return cmd


def build_ssh_command(host, remote_path, compose_cmd, identity_file=None):
    remote_sequence = (
        f"cd {shlex.quote(remote_path)} && "
        f"echo 'Preparing directories...' && "
        f"{compose_cmd} down && "
        f"{compose_cmd} build && "
        f"{compose_cmd} up -d"
    )
    cmd = ["ssh"]
    if identity_file:
        cmd.extend(["-i", identity_file])
    cmd.extend([host, remote_sequence])
    return cmd


def main():
    parser = argparse.ArgumentParser(description="Deploy lighter_MM components")
    parser.add_argument(
        "--host",
        default="ubuntu@54.95.246.213",
        help="SSH target in user@host form",
    )
    parser.add_argument(
        "--remote-path",
        default="/home/ubuntu/lighter_MM/",
        help="Remote directory to place files",
    )
    parser.add_argument(
        "--identity",
        default="lighter.pem",
        help="Path to SSH identity file (default: lighter.pem)",
    )
    parser.add_argument(
        "--compose-cmd",
        default="docker compose",
        help="Docker Compose command prefix (default: 'docker compose')",
    )
    parser.add_argument(
        "--files",
        nargs="*",
        default=[
            "market_maker_v2.py",
            "spread_calculator.py",
            "docker-compose.yml",
            "Dockerfile",
            "requirements.txt",
            ".env",
        ],
        help="Files and directories to upload",
    )

    args = parser.parse_args()

    local_files = []
    for file_name in args.files:
        path = pathlib.Path(file_name)
        if not path.exists():
            print(f"Missing required file or directory: {file_name}", file=sys.stderr)
            return 1
        local_files.append(path)

    identity_path = None
    if args.identity:
        identity_path = pathlib.Path(args.identity)
        if not identity_path.exists():
            print(f"SSH identity not found: {identity_path}", file=sys.stderr)
            return 1

    # SCP to copy files
    scp_cmd = build_scp_command(local_files, args.host, args.remote_path, str(identity_path) if identity_path else None)
    run_command(scp_cmd)

    # SSH to set permissions and restart docker
    ssh_cmd = build_ssh_command(args.host, args.remote_path, args.compose_cmd, str(identity_path) if identity_path else None)
    run_command(ssh_cmd)

    print("Deployment complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
