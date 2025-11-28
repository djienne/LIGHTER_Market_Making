"""Stops the Docker Compose stack on the remote host."""

import argparse
import pathlib
import shlex
import subprocess
import sys


def run_command(command):
    """Run a command and echo it to stdout."""
    print("Executing:", " ".join(shlex.quote(arg) for arg in command))
    subprocess.check_call(command)


def build_ssh_command(host, remote_path, compose_cmd, identity_file=None):
    """Builds the SSH command to stop the remote Docker stack."""
    remote_sequence = (
        f"cd {shlex.quote(remote_path)} && "
        f"{compose_cmd} down"
    )
    cmd = ["ssh"]
    if identity_file:
        cmd.extend(["-i", identity_file])
    cmd.extend([host, remote_sequence])
    return cmd


def main():
    """Parses arguments and executes the remote stop command."""
    parser = argparse.ArgumentParser(description="Stop the remote lighter_MM stack")
    parser.add_argument(
        "--host",
        default="ubuntu@54.95.246.213",
        help="SSH target in user@host form",
    )
    parser.add_argument(
        "--remote-path",
        default="/home/ubuntu/lighter_MM/",
        help="Remote directory of the docker-compose file",
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

    args = parser.parse_args()

    identity_path = None
    if args.identity:
        identity_path = pathlib.Path(args.identity)
        if not identity_path.exists():
            print(f"SSH identity not found: {identity_path}", file=sys.stderr)
            return 1

    # SSH to stop docker-compose
    ssh_cmd = build_ssh_command(
        args.host,
        args.remote_path,
        args.compose_cmd,
        str(identity_path) if identity_path else None,
    )
    run_command(ssh_cmd)

    print("Remote Docker stack stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
