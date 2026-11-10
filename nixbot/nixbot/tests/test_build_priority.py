"""Build priority: the configured priority of a build event."""

from __future__ import annotations

import pytest

from nixbot.config import BuildPriorityConfig, PriorityRule, priority_for
from nixbot.events import ChangeEvent, RepoInfo

CMS = RepoInfo(
    id=1,
    key="github/acme/cms",
    name="acme/cms",
    owner="acme",
    repo="cms",
    forge="github",
    clone_url="https://github.com/acme/cms.git",
    default_branch="main",
)
OTHER = RepoInfo(
    id=2,
    key="gitea/acme/tools",
    name="acme/tools",
    owner="acme",
    repo="tools",
    forge="gitea",
    clone_url="https://gitea.example.com/acme/tools.git",
    default_branch="main",
)

CONFIG = BuildPriorityConfig(
    rules=[
        PriorityRule(match="production", events=["push"], priority=20),
        PriorityRule(match="main", events=["push"], priority=10),
        PriorityRule(match="mergify/merge-queue/*", events=["push"], priority=5),
    ]
)


def _push(branch: str, repo: RepoInfo = CMS, tag: str | None = None) -> ChangeEvent:
    return ChangeEvent(repo=repo, branch=branch, commit_sha="a" * 40, tag=tag)


def _pr(base: str, head_ref: str = "feature") -> ChangeEvent:
    return ChangeEvent(
        repo=CMS, branch=base, commit_sha="b" * 40, pr_number=7, head_ref=head_ref
    )


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        (_push("production"), 20),
        (_push("main"), 10),
        (_push("mergify/merge-queue/main/pr-7"), 5),
        (_push("feature"), 0),
        # A pull request tests its base branch, and these rules are
        # push only.
        (_pr("main"), 0),
        (_pr("feature", head_ref="main"), 0),
    ],
)
def test_priority_for_rules(event: ChangeEvent, expected: int) -> None:
    assert priority_for(CONFIG, event) == expected


def test_first_matching_rule_wins() -> None:
    first = PriorityRule(match="release-*", priority=3)
    config = BuildPriorityConfig(
        rules=[first, PriorityRule(match="release-1", priority=9)]
    )
    assert priority_for(config, _push("release-1")) == first.priority


def test_repo_glob() -> None:
    rule = PriorityRule(repo="github/acme/*", match="main", priority=4)
    config = BuildPriorityConfig(rules=[rule])
    assert priority_for(config, _push("main")) == rule.priority
    assert priority_for(config, _push("main", repo=OTHER)) == 0


def test_pull_request_matches_base_branch() -> None:
    rule = PriorityRule(match="main", events=["pull_request"], priority=2)
    config = BuildPriorityConfig(rules=[rule])
    assert priority_for(config, _pr("main")) == rule.priority
    assert priority_for(config, _pr("staging", head_ref="main")) == 0
    # A pull_request-only rule does not match pushes.
    assert priority_for(config, _push("main")) == 0


def test_tag_push_matches_on_the_tag() -> None:
    rule = PriorityRule(match="v*", events=["push"], priority=6)
    config = BuildPriorityConfig(rules=[rule])
    assert priority_for(config, _push("v1.2.0", tag="v1.2.0")) == rule.priority


def test_default_is_zero() -> None:
    assert priority_for(None, _push("production")) == 0
    assert priority_for(BuildPriorityConfig(), _push("production")) == 0
