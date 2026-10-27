"""Per-repository config file loading.

Repositories migrating from buildbot-nix still carry a
`buildbot-nix.toml`. It must keep working until renamed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nixbot.repo_config import BranchConfig

if TYPE_CHECKING:
    from pathlib import Path


def test_load_nixbot_toml(tmp_path: Path) -> None:
    (tmp_path / "nixbot.toml").write_text('attribute = "hydraJobs"')
    assert BranchConfig.load(tmp_path).attribute == "hydraJobs"


def test_load_legacy_buildbot_nix_toml(tmp_path: Path) -> None:
    (tmp_path / "buildbot-nix.toml").write_text('attribute = "hydraJobs"')
    assert BranchConfig.load(tmp_path).attribute == "hydraJobs"


def test_nixbot_toml_wins_over_legacy(tmp_path: Path) -> None:
    (tmp_path / "nixbot.toml").write_text('attribute = "new"')
    (tmp_path / "buildbot-nix.toml").write_text('attribute = "old"')
    assert BranchConfig.load(tmp_path).attribute == "new"


def test_missing_files_default(tmp_path: Path) -> None:
    assert BranchConfig.load(tmp_path).attribute == "checks"


def test_branch_config_defaults(tmp_path: Path) -> None:
    config = BranchConfig.load(tmp_path)
    assert config.flake_dir == "."
    assert config.lock_file == "flake.lock"
    assert config.attribute == "checks"
    assert not config.effects_on_pull_requests


def test_branch_config_from_toml(tmp_path: Path) -> None:
    (tmp_path / "nixbot.toml").write_text(
        'flake_dir = "subdir"\nlock_file = "dev.lock"\nattribute = "hydraJobs"\n'
        "effects_on_pull_requests = true\n"
    )
    config = BranchConfig.load(tmp_path)
    assert config.flake_dir == "subdir"
    assert config.lock_file == "dev.lock"
    assert config.attribute == "hydraJobs"
    assert config.effects_on_pull_requests


def test_build_branches_default_is_none(tmp_path: Path) -> None:
    assert BranchConfig.load(tmp_path).build_branches is None


def test_build_branches_from_toml(tmp_path: Path) -> None:
    (tmp_path / "nixbot.toml").write_text('build_branches = ["release-*"]\n')
    assert BranchConfig.load(tmp_path).build_branches == ["release-*"]


def test_branch_config_rejects_traversal(tmp_path: Path) -> None:
    (tmp_path / "nixbot.toml").write_text('flake_dir = "../../etc"\n')
    # Falls back to defaults on invalid config.
    assert BranchConfig.load(tmp_path).flake_dir == "."


def test_branch_config_invalid_toml(tmp_path: Path) -> None:
    (tmp_path / "nixbot.toml").write_text("not toml :::")
    assert BranchConfig.load(tmp_path).flake_dir == "."


def test_attribute_branches_default_is_empty(tmp_path: Path) -> None:
    assert BranchConfig.load(tmp_path).attribute_branches == []


def test_resolve_attribute_without_rules() -> None:
    config = BranchConfig.loads('attribute = "hydraJobs"\n')
    assert config.resolve_attribute("main", is_pull_request=False) == "hydraJobs"
    assert config.resolve_attribute("main", is_pull_request=True) == "hydraJobs"


def test_resolve_attribute_pull_request_rule() -> None:
    config = BranchConfig.loads(
        "[[attribute_branches]]\n"
        'match = "*"\nattribute = "fast"\nevents = ["pull_request"]\n'
    )
    assert config.resolve_attribute("main", is_pull_request=True) == "fast"
    assert config.resolve_attribute("main", is_pull_request=False) == "checks"


def test_resolve_attribute_push_rule() -> None:
    config = BranchConfig.loads(
        "[[attribute_branches]]\n"
        'match = "production"\nattribute = "release"\nevents = ["push"]\n'
    )
    assert config.resolve_attribute("production", is_pull_request=False) == "release"
    # A pull request is matched by its base branch, and this rule
    # does not cover pull requests.
    assert config.resolve_attribute("production", is_pull_request=True) == "checks"
    assert config.resolve_attribute("main", is_pull_request=False) == "checks"


def test_resolve_attribute_first_rule_wins() -> None:
    config = BranchConfig.loads(
        "[[attribute_branches]]\n"
        'match = "release-*"\nattribute = "first"\n'
        "[[attribute_branches]]\n"
        'match = "*"\nattribute = "second"\n'
    )
    assert config.resolve_attribute("release-1", is_pull_request=False) == "first"
    assert config.resolve_attribute("main", is_pull_request=False) == "second"


def test_resolve_attribute_source_match() -> None:
    config = BranchConfig.loads(
        "[[attribute_branches]]\n"
        'match = "main"\nsource_match = "update-*"\nattribute = "full"\n'
        "[[attribute_branches]]\n"
        'match = "*"\nattribute = "fast"\nevents = ["pull_request"]\n'
    )

    def resolve(branch: str, head_ref: str | None) -> str:
        return config.resolve_attribute(branch, is_pull_request=True, head_ref=head_ref)

    assert resolve("main", "update-nixpkgs") == "full"
    assert resolve("main", "feature") == "fast"
    # Both globs must match.
    assert resolve("release", "update-nixpkgs") == "fast"
    # An event without a head branch (e.g. reconstructed from a
    # build) does not match a source_match rule.
    assert resolve("main", None) == "fast"
    # A push has no source branch, even with the rule's events
    # defaulting to both.
    assert (
        config.resolve_attribute(
            "main", is_pull_request=False, head_ref="update-nixpkgs"
        )
        == "checks"
    )


def test_resolve_attribute_events_omitted_means_both() -> None:
    config = BranchConfig.loads(
        '[[attribute_branches]]\nmatch = "main"\nattribute = "packages"\n'
    )
    assert config.resolve_attribute("main", is_pull_request=False) == "packages"
    assert config.resolve_attribute("main", is_pull_request=True) == "packages"


def test_attribute_branches_unknown_event_falls_back_to_defaults() -> None:
    config = BranchConfig.loads(
        'attribute = "hydraJobs"\n'
        "[[attribute_branches]]\n"
        'match = "*"\nattribute = "fast"\nevents = ["merge_group"]\n'
    )
    # Falls back to defaults on invalid config.
    assert config == BranchConfig()
    assert config.resolve_attribute("main", is_pull_request=True) == "checks"


def test_attribute_branches_missing_attribute_falls_back_to_defaults(
    tmp_path: Path,
) -> None:
    (tmp_path / "nixbot.toml").write_text(
        'attribute = "hydraJobs"\n[[attribute_branches]]\nmatch = "*"\n'
    )
    config = BranchConfig.load(tmp_path)
    assert config == BranchConfig()
    assert config.resolve_attribute("main", is_pull_request=False) == "checks"


def test_resolve_attribute_merge_queue_push_without_matching_rule() -> None:
    config = BranchConfig.loads(
        'attribute = "hydraJobs"\n'
        "[[attribute_branches]]\n"
        'match = "production"\nattribute = "release"\nevents = ["push"]\n'
    )
    branch = "gh-readonly-queue/main/pr-1-abc"
    assert config.resolve_attribute(branch, is_pull_request=False) == "hydraJobs"


def test_protect_grace_default_is_none(tmp_path: Path) -> None:
    config = BranchConfig.load(tmp_path)
    assert config.protect_running_branches == []
    assert config.protect_grace("main") is None


def test_protect_grace_glob_match() -> None:
    grace = 300
    config = BranchConfig.loads(
        f'[[protect_running_branches]]\nmatch = "release-*"\ngrace_seconds = {grace}\n'
    )
    assert config.protect_grace("release-1") == grace
    assert config.protect_grace("main") is None


def test_protect_grace_first_rule_wins() -> None:
    first, second = 60, 600
    config = BranchConfig.loads(
        "[[protect_running_branches]]\n"
        f'match = "main"\ngrace_seconds = {first}\n'
        "[[protect_running_branches]]\n"
        f'match = "*"\ngrace_seconds = {second}\n'
    )
    assert config.protect_grace("main") == first
    assert config.protect_grace("production") == second


def test_protect_running_branches_invalid_falls_back_to_defaults() -> None:
    for rule in (
        'match = "main"\ngrace_seconds = -1\n',
        'match = "main"\ngrace_seconds = "soon"\n',
        'match = "main"\n',
    ):
        config = BranchConfig.loads(
            f'attribute = "hydraJobs"\n[[protect_running_branches]]\n{rule}'
        )
        # Falls back to defaults on invalid config.
        assert config == BranchConfig()
        assert config.protect_grace("main") is None


# The rules cms ships: pull requests build the fast set, `production`
# builds the full set under its own attribute, everything else builds
# `checks`.
CMS_CONFIG = """\
attribute = "checks"

[[attribute_branches]]
match = "*"
attribute = "fastChecks"
events = ["pull_request"]

[[attribute_branches]]
match = "production"
attribute = "releaseChecks"
events = ["push"]
"""


def test_resolve_attribute_cms_configuration() -> None:
    config = BranchConfig.loads(CMS_CONFIG)
    for base in ("main", "production", "feature/x"):
        assert config.resolve_attribute(base, is_pull_request=True) == "fastChecks"
    assert (
        config.resolve_attribute("production", is_pull_request=False) == "releaseChecks"
    )
    for branch in (
        "main",
        "gh-readonly-queue/main/pr-1-abc",
        "v1.2.3",
        "release-1",
    ):
        assert config.resolve_attribute(branch, is_pull_request=False) == "checks"
    # Missing or invalid file: the full set.
    for text in (None, "not toml :::"):
        fallback = BranchConfig.loads(text)
        assert fallback.resolve_attribute("main", is_pull_request=True) == "checks"
        assert fallback.resolve_attribute("main", is_pull_request=False) == "checks"
