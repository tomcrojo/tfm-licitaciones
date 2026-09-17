"""Static boundary tests: production/experiment isolation for the Silver comparison."""

from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent.parent
SRC = ROOT / "src" / "tfm_licitaciones"
EXPERIMENT = ROOT / "experiments" / "silver_engine_comparison"

_IMPORT_RE = re.compile(r"^\s*(?:from|import)\s+([a-zA-Z0-9_.]+)", re.MULTILINE)

# Production Silver facade/reference modules that must never serve as the
# historical ``python-row`` oracle. ``tfm_licitaciones.silver_parity`` is
# intentionally NOT here: it only compares frames, it does not define the
# baseline transform.
_FORBIDDEN_FACADE_MODULES = frozenset(
    {
        "tfm_licitaciones.silver",
        "tfm_licitaciones.silver_reference",
        "tfm_licitaciones.silver_native",
    }
)


def _ast_imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                modules.add(node.module)
    return modules


def _is_forbidden_facade(module: str) -> bool:
    return any(
        module == forbidden or module.startswith(forbidden + ".")
        for forbidden in _FORBIDDEN_FACADE_MODULES
    )


class ProductionBoundaryTests(unittest.TestCase):
    """Nothing under ``src/`` may depend on ``experiments/``."""

    def test_production_never_imports_experiments(self) -> None:
        offenders = []
        for path in sorted(SRC.rglob("*.py")):
            for match in _IMPORT_RE.finditer(path.read_text(encoding="utf-8")):
                module = match.group(1)
                if module == "experiments" or module.startswith("experiments."):
                    offenders.append(f"{path.relative_to(ROOT)}: {module}")
        self.assertEqual(offenders, [])

    def test_experiment_modules_do_not_live_under_src(self) -> None:
        for name in ("silver_polars.py", "silver_spark.py", "bench_engines.py"):
            self.assertFalse(
                (SRC / name).exists(),
                f"experiment module {name} must not live under src/",
            )
        self.assertTrue((EXPERIMENT / "bench_engines.py").is_file())
        self.assertTrue((EXPERIMENT / "engines" / "polars_candidate.py").is_file())
        self.assertTrue((EXPERIMENT / "engines" / "spark_candidate.py").is_file())
        self.assertTrue((EXPERIMENT / "engines" / "python_row_reference.py").is_file())


class ExperimentIsolationTests(unittest.TestCase):
    """The experiment must not track the mutable production Silver facade.

    The ``python-row`` engine is frozen from e69016f in
    ``engines/python_row_reference.py``. After PR #17 the production facade
    becomes native Polars: any experiment import of that facade as the
    benchmark oracle would silently relabel Polars as ``python-row``.
    """

    def test_experiment_never_imports_production_silver_facade(self) -> None:
        offenders = []
        for path in sorted(EXPERIMENT.rglob("*.py")):
            for module in sorted(_ast_imported_modules(path)):
                if _is_forbidden_facade(module):
                    offenders.append(f"{path.relative_to(ROOT)}: {module}")
        self.assertEqual(offenders, [])

    def test_frozen_reference_has_no_production_imports(self) -> None:
        modules = _ast_imported_modules(EXPERIMENT / "engines" / "python_row_reference.py")
        offenders = sorted(
            module
            for module in modules
            if module == "tfm_licitaciones" or module.startswith("tfm_licitaciones.")
        )
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
