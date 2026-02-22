"""
Launcher script that handles Cyrillic path issues on Windows.
Place in a parent directory or use: python launch.py [command]
"""
import os
import sys
import importlib


def find_project_dir():
    """Find the project directory (handles Cyrillic paths)."""
    # Try direct path first
    candidates = [
        os.path.dirname(os.path.abspath(__file__)),
        os.path.join("d:\\VibeCoding", [
            d for d in os.listdir("d:\\VibeCoding") if not d.isascii()
        ][0]) if os.path.exists("d:\\VibeCoding") else None,
    ]

    for path in candidates:
        if path and os.path.exists(os.path.join(path, "run.py")):
            return path

    return os.getcwd()


def main():
    project_dir = find_project_dir()
    os.chdir(project_dir)

    if project_dir not in sys.path:
        sys.path.insert(0, project_dir)

    import run
    run.main()


if __name__ == "__main__":
    main()
