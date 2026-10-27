"""Per-repository configuration (`nixbot.toml`), read from the
build worktree instead of via a buildbot remote command."""

from __future__ import annotations

import tomllib
from fnmatch import fnmatch
from typing import TYPE_CHECKING, Literal, Self

from pydantic import BaseModel, NonNegativeInt, ValidationError

if TYPE_CHECKING:
    from pathlib import Path


# Preference order. The legacy buildbot-nix name keeps repositories
# migrating from buildbot-nix working without a rename.
CONFIG_FILENAMES = ("nixbot.toml", "buildbot-nix.toml")


class RepoConfigError(Exception):
    pass


def _validate_flake_dir(flake_dir: str, repo_root: Path) -> None:
    """Validate that flake_dir is a safe relative path within the repo root."""
    resolved = (repo_root / flake_dir).resolve()
    if ":" in flake_dir or not resolved.is_relative_to(repo_root.resolve()):
        msg = f"Invalid flake_dir {flake_dir}"
        raise RepoConfigError(msg)


AttributeEvent = Literal["push", "pull_request"]


class AttributeBranchRule(BaseModel):
    # Branch glob. For a pull request it tests the base branch.
    match: str
    attribute: str
    events: list[AttributeEvent] = ["push", "pull_request"]
    # Pull request head branch glob, tested alongside `match`. A push
    # never matches a rule that sets it.
    source_match: str | None = None


class ProtectRunningRule(BaseModel):
    # Branch glob, tested against the pushed branch.
    match: str
    # How long a build runs before newer pushes stop superseding it.
    grace_seconds: NonNegativeInt


class BranchConfig(BaseModel):
    flake_dir: str = "."
    lock_file: str = "flake.lock"
    attribute: str = "checks"
    # Per-branch replacements for `attribute`. The first rule whose
    # glob and events match wins. No match means `attribute` applies.
    attribute_branches: list[AttributeBranchRule] = []
    # Running push builds of matching branches are not superseded once
    # they have run `grace_seconds`. The first rule whose glob matches
    # wins.
    protect_running_branches: list[ProtectRunningRule] = []
    effects_on_pull_requests: bool = False
    effects_branches: list[str] = []
    # Branch globs whose pushes build, replacing the globally
    # configured branch globs. Default branch and merge-queue
    # branches always build. None means the global config applies.
    build_branches: list[str] | None = None
    # False opts out of the instance-wide contributor approval gate.
    require_approval: bool = True

    def resolve_attribute(
        self, branch: str, *, is_pull_request: bool, head_ref: str | None = None
    ) -> str:
        """Flake attribute to build for an event on `branch` (the base
        branch for a pull request, whose head branch is `head_ref`)."""
        event: AttributeEvent = "pull_request" if is_pull_request else "push"
        for rule in self.attribute_branches:
            if event not in rule.events or not fnmatch(branch, rule.match):
                continue
            if rule.source_match is not None and (
                not is_pull_request
                or head_ref is None
                or not fnmatch(head_ref, rule.source_match)
            ):
                continue
            return rule.attribute
        return self.attribute

    def protect_grace(self, branch: str) -> int | None:
        """Seconds after which a push build of `branch` is no longer
        superseded. None means it always is."""
        for rule in self.protect_running_branches:
            if fnmatch(branch, rule.match):
                return rule.grace_seconds
        return None

    @classmethod
    def loads(cls, text: str | None) -> Self:
        """Parse `nixbot.toml` content (e.g. read from a git ref);
        defaults on absence or invalid content."""
        if text is None:
            return cls()
        try:
            return cls.model_validate(tomllib.loads(text))
        except (tomllib.TOMLDecodeError, ValidationError):
            return cls()

    @classmethod
    def load(cls, repo_root: Path) -> Self:
        """Read `nixbot.toml` (or the legacy `buildbot-nix.toml`)
        from a checkout. Defaults on absence or invalid content
        (matching the buildbot-era behavior)."""
        for filename in CONFIG_FILENAMES:
            try:
                text = (repo_root / filename).read_text()
            except OSError:
                continue
            break
        else:
            return cls()
        config = cls.loads(text)
        try:
            _validate_flake_dir(config.flake_dir, repo_root)
        except RepoConfigError:
            return cls()
        return config
