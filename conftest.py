"""The suite budget and the slow lane.

`suite_budget` in `[tool.pytest.ini_options]` is the most seconds a run may take, and
`timeout`, read by pytest-timeout, the most one test may take. A test marked `slow` is held
to neither, and the default run deselects it. So is a `network` test: no time cap fixes a wait
on live NCBI.
"""

from __future__ import annotations

import time

import pytest

SLOW = "slow"
EXEMPT = (SLOW, "network")


def _exempt(item: pytest.Item) -> bool:
    """Whether a test carries a mark that frees it from both limits."""
    return any(item.get_closest_marker(name) for name in EXEMPT)


class SuiteBudget:
    """Times one run, less its exempt tests, and fails it when that is over budget."""

    def __init__(self, budget: float) -> None:
        self.budget = budget
        self.started = time.perf_counter()
        self.exempt_ids: set[str] = set()
        self.exempt_seconds = 0.0
        self.over = False
        self.verdict = ""

    def pytest_sessionstart(self) -> None:
        """Start the clock where pytest starts its own."""
        self.started = time.perf_counter()

    def pytest_collection_modifyitems(self, items: list[pytest.Item]) -> None:
        """Note which tests carry a mark. Keywords would also match a directory named `slow`."""
        self.exempt_ids = {item.nodeid for item in items if _exempt(item)}

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        """Count an exempt test's setup, call and teardown, so the budget leaves them out."""
        if report.nodeid in self.exempt_ids:
            self.exempt_seconds += report.duration

    @pytest.hookimpl(trylast=True)
    def pytest_sessionfinish(self, session: pytest.Session) -> None:
        """Fail a run that took longer than its budget."""
        took = time.perf_counter() - self.started - self.exempt_seconds
        excluded = (
            f" ({self.exempt_seconds:.1f} s of exempt tests not counted)"
            if self.exempt_seconds
            else ""
        )
        self.over = took > self.budget
        if not self.over:
            self.verdict = f"suite time: {took:.1f} s of its {self.budget:g} s budget{excluded}"
            return
        self.verdict = (
            f"suite time: {took:.1f} s, over its {self.budget:g} s budget{excluded}. "
            "Find the slowest tests with --durations=10, then make them faster or mark them slow."
        )
        if session.exitstatus == pytest.ExitCode.OK:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED

    def pytest_terminal_summary(self, terminalreporter: pytest.TerminalReporter) -> None:
        """Print the time against the budget."""
        terminalreporter.write_line(self.verdict, red=self.over, bold=self.over)


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register `suite_budget`."""
    parser.addini(
        "suite_budget",
        "Most seconds a run may take, exempt tests excluded. 0 or unset: no budget.",
        type="float",
        default=0.0,
    )


def pytest_configure(config: pytest.Config) -> None:
    """Register the `slow` marker, and the budget when one is set."""
    config.addinivalue_line(
        "markers",
        f"{SLOW}: exempt from timeout and suite_budget, and deselected unless -m {SLOW}",
    )
    budget = config.getini("suite_budget")
    if budget:
        config.pluginmanager.register(SuiteBudget(budget), "suite-budget")


def pytest_timeout_set_timer(item: pytest.Item) -> bool | None:
    """Set no timer for an exempt test; pytest-timeout sets one for every other."""
    return True if _exempt(item) else None
