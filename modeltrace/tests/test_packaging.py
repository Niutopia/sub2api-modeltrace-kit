from __future__ import annotations

from pathlib import Path


BASE_DIGEST = "sha256:44ff437bba879d4941b710a369a8f19266aea34b29002807f0c487fabc9eec9b"


def _non_comment_lines(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def test_runtime_requirements_are_pinned_and_match_lock():
    requirements = _non_comment_lines(Path("requirements.txt"))
    lock = _non_comment_lines(Path("requirements.lock.txt"))

    assert requirements == lock
    assert requirements
    assert all("==" in line and not any(operator in line for operator in (">=", "<=", ">", "<")) for line in requirements)


def test_docker_uses_verified_base_digest_and_runtime_lock():
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    assert f"FROM python:3.12-slim@{BASE_DIGEST}" in dockerfile
    assert "COPY requirements.lock.txt ./requirements.lock.txt" in dockerfile
    assert "-r requirements.lock.txt" in dockerfile
    assert "-r requirements.txt" not in dockerfile
