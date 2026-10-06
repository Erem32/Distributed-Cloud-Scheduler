"""Run the project's quality checks with the active Python interpreter."""

import subprocess
import sys
from pathlib import Path


def main() -> int:
    """Stop at the first failed check and return its exit status."""
    project_root = Path(__file__).resolve().parents[1]
    checks = (
        ("ruff", "check", "."),
        ("mypy", "src"),
        ("pytest",),
    )
    for arguments in checks:
        command = [sys.executable, "-m", *arguments]
        print(f"Running: {' '.join(command)}", flush=True)
        result = subprocess.run(command, cwd=project_root, check=False)
        if result.returncode != 0:
            return result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
