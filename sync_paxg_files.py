"""Simple file synchronization helper for lighter_MM.

Copies the specified Python files to the remote host via SCP.
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
    cmd = ["scp"]
    if identity_file:
        cmd.extend(["-i", identity_file])
    cmd.extend(path.as_posix() for path in files)
    cmd.append(f"{host}:{remote_path}")
    return cmd


def build_pre_ssh_command(host, remote_path, identity_file=None):
    """Builds an SSH command to prepare the remote directory."""
    parent_dir = shlex.quote(pathlib.Path(remote_path).parent.as_posix())
    remote_sequence = (
        f"mkdir -p {shlex.quote(remote_path)} && "
        f"sudo chown -R $(whoami):$(whoami) {parent_dir} && "
        f"sudo chmod -R a+rw {shlex.quote(remote_path)}"
    )
    cmd = ["ssh"]
    if identity_file:
        cmd.extend(["-i", identity_file])
    cmd.extend([host, remote_sequence])
    return cmd


def main():
    parser = argparse.ArgumentParser(description="Sync PAXG data files to a remote host")
    parser.add_argument(
        "--host",
        default="ubuntu@54.95.246.213",
        help="SSH target in user@host form",
    )
    parser.add_argument(
        "--remote-path",
        default="/home/ubuntu/lighter_MM/lighter_data/",
        help="Remote directory to place files",
    )
    parser.add_argument(
        "--identity",
        default="lighter.pem",
        help="Path to SSH identity file (default: lighter.pem)",
    )
    parser.add_argument(
        "--files",
        nargs="*",
        default=[
            "lighter_data/prices_PAXG.csv",
            "lighter_data/trades_PAXG.csv",
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

    # PRE-FLIGHT SSH: Ensure remote directory exists and has correct ownership/permissions
    pre_ssh_cmd = build_pre_ssh_command(args.host, args.remote_path, str(identity_path) if identity_path else None)
    run_command(pre_ssh_cmd)

    # Run SCP to copy files and directories
    scp_cmd = build_scp_command(local_files, args.host, args.remote_path, str(identity_path) if identity_path else None)
    run_command(scp_cmd)

    print("File synchronization and permission setting complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
