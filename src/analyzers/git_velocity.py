from __future__ import annotations
import subprocess
from pathlib import Path
from collections import defaultdict
from loguru import logger

import threading
from typing import Dict, Optional

class ModuleVelocity(int):
    """
    An integer representing change count, but with an extra flag 
    indicating if a deep audit is required.
    """
    def __new__(cls, value, deep_audit_required=False):
        obj = super(ModuleVelocity, cls).__new__(cls, value)
        obj.deep_audit_required = deep_audit_required
        return obj

# Global cache for git velocity results
# Key: (repo_root_str, days) -> result_dict
_velocity_cache: Dict[tuple[str, int], dict[str, ModuleVelocity]] = {}
_cache_lock = threading.Lock()

def adjust_confidence(base_confidence: float, change_velocity: int, page_rank: float, threshold: int = 50) -> float:
    """
    Reduce confidence if module is high velocity and high importance.
    High PageRank (importance) + High Velocity (instability) = Higher risk/Lower confidence.
    """
    if change_velocity > threshold:
        # Scale deduction up to 30% based on importance
        deduction = min(0.3, page_rank)
        return max(0.1, base_confidence * (1 - deduction))
    return base_confidence

def extract_git_velocity(
    repo_root: Path, 
    days: int = 30, 
    deep_audit_threshold: int = 50,
    use_cache: bool = True
) -> dict[str, ModuleVelocity]:
    """
    Return mapping of repo-relative POSIX file paths -> ModuleVelocity
    based on `git log` over the last `days`.
    
    If change_count > deep_audit_threshold, the ModuleVelocity will have 
    deep_audit_required=True.
    """
    cache_key = (str(repo_root.resolve()), days)
    
    if use_cache:
        with _cache_lock:
            if cache_key in _velocity_cache:
                return _velocity_cache[cache_key]

    velocity_counts: dict[str, int] = defaultdict(int)
    
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
            
            try:
                # The paths from git log are already relative to repo root
                normalized_path = Path(line).as_posix()
                velocity_counts[normalized_path] += 1
            except Exception as e:
                logger.debug(f"Failed to normalize path '{line}': {e}")
                continue

    except FileNotFoundError:
        logger.warning("git command not found. Ensure git is installed and in PATH.")
        return {}
    except subprocess.CalledProcessError as e:
        logger.error(f"git log failed with exit code {e.returncode}: {e.stderr}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error extracting git velocity: {e}")
        return {}

    # Convert to ModuleVelocity objects
    results: dict[str, ModuleVelocity] = {}
    for path, count in velocity_counts.items():
        is_deep = count > deep_audit_threshold
        if is_deep:
            logger.info(f"[git_velocity] High velocity detected for {path}: {count} changes. Deep Audit flagged.")
        results[path] = ModuleVelocity(count, deep_audit_required=is_deep)

    if use_cache:
        with _cache_lock:
            _velocity_cache[cache_key] = results

    return results
