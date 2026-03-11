from __future__ import annotations
import subprocess
from pathlib import Path
from collections import defaultdict
from loguru import logger

def extract_git_velocity(repo_root: Path, days: int = 30) -> dict[str, int]:
    """
    Return mapping of repo-relative POSIX file paths -> change_count
    based on `git log` over the last `days`.
    """
    velocity: dict[str, int] = defaultdict(int)
    
    # If .git does not exist, return empty dict
    git_dir = repo_root / ".git"
    if not git_dir.exists():
        logger.debug(f"No .git directory found at {repo_root}")
        return {}

    try:
        # git log --since="<days> days ago" --name-only --pretty=format:
        result = subprocess.run(
            [
                "git", "-C", str(repo_root),
                "log", f"--since={days} days ago",
                "--name-only", "--pretty=format:",
            ],
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8",
            errors="replace"
        )
        
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            
            # Normalize paths to repo-relative POSIX
            try:
                # The paths from git log are already relative to repo root
                normalized_path = Path(line).as_posix()
                velocity[normalized_path] += 1
            except Exception as e:
                logger.debug(f"Failed to normalize path '{line}': {e}")
                continue

    except FileNotFoundError:
        # Git is not installed
        logger.warning("git command not found. Ensure git is installed and in PATH.")
        return {}
    except subprocess.CalledProcessError as e:
        logger.error(f"git log failed with exit code {e.returncode}: {e.stderr}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error extracting git velocity: {e}")
        return {}

    return dict(velocity)
