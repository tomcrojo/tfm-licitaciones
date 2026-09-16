"""Static boundary test: production code must never import experiments."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent.parent
SRC = ROOT / "src" / "tfm_licitaciones"
EXPERIMENT = ROOT / "experiments" / "silver_engine_comparison"

_IMPORT_RE = re.compile(r"^\s*(?:from|import)\s+([a-zA-Z0-9_.]+)", re.MULTILINE)


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


if __name__ == "__main__":
    unittest.main()
