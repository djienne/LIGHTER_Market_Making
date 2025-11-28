"""
Starts only the lighter-data-collector service using Docker Compose.
"""

import subprocess
import sys

def run_command(command):
    """Run a command and check for errors."""
    print(f"Executing: {' '.join(command)}")
    try:
        subprocess.check_call(command)
        print("Command executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print("Error: 'docker' command not found. Is Docker installed and in your PATH?", file=sys.stderr)
        sys.exit(1)

def main():
    """Main function to start the service."""
    print("Building and starting the 'lighter-data-collector' service...")
    
    # Command to build and start the specific service in detached mode
    compose_command = [
        "docker",
        "compose",
        "up",
        "--build",
        "-d",
        "lighter-data-collector"
    ]
    
    run_command(compose_command)
    
    print("\nService 'lighter-data-collector' is starting in the background.")
    print("You can view logs with: docker compose logs -f lighter-data-collector")

if __name__ == "__main__":
    main()

