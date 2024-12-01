from __future__ import annotations

import os
import pathlib
import shutil
import tempfile

import nox  # type: ignore
import packaging.version


def fetch_latest_tag(session: nox.Session) -> packaging.version.Version:
    if "LATEST_TAG" in os.environ:
        return packaging.version.Version(os.environ["LATEST_TAG"])

    session.run("git", "fetch", "--tags", external=True)
    out = session.run("git", "tag", "--list", external=True, silent=True)
    assert out
    return max(packaging.version.Version(tag) for tag in out.splitlines())


def get_pre_migration(latest_tag: packaging.version.Version) -> str:
    migrations_folder = (
        pathlib.Path(__file__).parent / "procrastinate" / "sql" / "migrations"
    )
    migrations = sorted(migrations_folder.glob("*.sql"))
    pre_migration: pathlib.Path | None = None
    for migration in migrations:
        mig_version = packaging.version.Version(migration.name.split("_")[0])
        if mig_version > latest_tag and "_post_" in migration.name:
            break

        pre_migration = migration

    assert pre_migration is not None
    return pre_migration.name


@nox.session
def current_version_with_post_migration(session: nox.Session):
    session.install("poetry")
    session.run("poetry", "install", "--all-extras")
    session.run(
        "poetry",
        "run",
        "pytest",
        *session.posargs,
    )


@nox.session
def current_version_without_post_migration(session: nox.Session):
    latest_tag = fetch_latest_tag(session)
    pre_migration = get_pre_migration(latest_tag)

    session.install("poetry")
    session.run("poetry", "install", "--all-extras")
    session.run(
        "poetry",
        "run",
        "pytest",
        f"--migrate-until={pre_migration}",
        "./tests/acceptance",
        *session.posargs,
    )


@nox.session
def stable_version_without_post_migration(session: nox.Session):
    latest_tag = fetch_latest_tag(session)
    pre_migration = get_pre_migration(latest_tag)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = pathlib.Path(temp_dir)
        base_path = pathlib.Path(__file__).parent
        shutil.copytree(base_path / "tests", temp_path / "tests")
        shutil.copy(base_path / "pyproject.toml", temp_path / "pyproject.toml")
        shutil.copy(base_path / "poetry.lock", temp_path / "poetry.lock")
        session.chdir(temp_dir)

        session.install("poetry")
        session.install("poetry-plugin-export")
        session.run(
            "poetry",
            "export",
            "--output",
            "requirements.txt",
            "--with",
            "test",
            "--all-extras",
            "--without-hashes",
            env={"POETRY_WARNINGS_EXPORT": "false"},
        )
        session.install("-r", "requirements.txt")
        session.install("procrastinate")
        session.run(
            "pytest",
            f"--migrate-until={pre_migration}",
            f"--latest-version={latest_tag}",
            "./tests/acceptance",
            *session.posargs,
            env={"PYTHONPATH": temp_dir},
        )
