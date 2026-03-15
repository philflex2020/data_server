"""
Shared battery cell simulation core.

Used by both the simple MQTT generator (orig_aws/source/generator.py)
and the multi-project stress runner (source/stress_runner/stress_runner.py).

Each cell performs a Gaussian random-walk with drift reversal at bounds,
producing realistic-looking sensor trajectories.

Schema:
  cell_data is a dict of  name -> {min, max, nominal?, drift?}
  where drift is the max absolute step per sample (default 0.5% of range).

  Different projects can supply different cell_data dicts — that is how
  per-project measurement schemas are expressed.
"""

import random
import time
from dataclasses import dataclass, field
from typing import Dict, List


# ---------------------------------------------------------------------------
# Default measurement schema (used when no cell_data is provided in config)
# ---------------------------------------------------------------------------

DEFAULT_CELL_DATA: Dict[str, dict] = {
    # name:            min     max     nominal  drift (abs step)
    "voltage":      {"min": 3.0,   "max": 4.2,   "nominal": 3.7,   "drift": 0.005},
    "current":      {"min": -10.0, "max": 10.0,  "nominal": 0.0,   "drift": 0.2},
    "temperature":  {"min": 15.0,  "max": 45.0,  "nominal": 28.0,  "drift": 0.1},
    "soc":          {"min": 10.0,  "max": 95.0,  "nominal": 70.0,  "drift": 0.05},
    "soh":          {"min": 80.0,  "max": 100.0, "nominal": 98.0,  "drift": 0.01},
    "resistance":   {"min": 0.001, "max": 0.01,  "nominal": 0.003, "drift": 0.00005},
    "capacity":     {"min": 80.0,  "max": 105.0, "nominal": 100.0, "drift": 0.01},
}


# ---------------------------------------------------------------------------
# Simulation primitives
# ---------------------------------------------------------------------------

@dataclass
class MeasurementState:
    """Simulated value for one measurement field on one cell.

    Uses a Gaussian random-walk: drift accumulates and reverses at bounds,
    producing smooth, bounded trajectories.
    """
    value:   float
    min_val: float
    max_val: float
    drift:   float   # current step size (signed)

    def step(self) -> float:
        self.drift += random.gauss(0, abs(self.drift) * 0.1 + 1e-6)
        max_step = (self.max_val - self.min_val) * 0.02
        self.drift = max(-max_step, min(max_step, self.drift))
        self.value += self.drift
        if self.value > self.max_val:
            self.value = self.max_val
            self.drift = -abs(self.drift)
        elif self.value < self.min_val:
            self.value = self.min_val
            self.drift = abs(self.drift)
        return round(self.value, 5)


@dataclass
class CellState:
    """All simulated measurement state for one physical cell."""
    project_id:   int
    site_id:      int
    rack_id:      int
    module_id:    int
    cell_id:      int
    measurements: Dict[str, MeasurementState] = field(default_factory=dict)

    @property
    def topic(self) -> str:
        return (
            f"batteries/project={self.project_id}/site={self.site_id}"
            f"/rack={self.rack_id}/module={self.module_id}/cell={self.cell_id}"
        )

    def payload(self) -> dict:
        values = {name: state.step() for name, state in self.measurements.items()}
        p = {
            "timestamp":  time.time(),
            "project_id": self.project_id,
            "site_id":    self.site_id,
            "rack_id":    self.rack_id,
            "module_id":  self.module_id,
            "cell_id":    self.cell_id,
            **values,
        }
        # Derived field — only when both voltage and current are in the schema
        if "voltage" in values and "current" in values:
            p["power"] = round(values["voltage"] * values["current"], 4)
        return p


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def build_cells(
    project_id:       int,
    site_id:          int,
    racks:            int,
    modules_per_rack: int,
    cells_per_module: int,
    cell_data:        Dict[str, dict],
) -> List[CellState]:
    """Build CellState list for one (project_id, site_id) pair.

    cell_data maps measurement name -> {min, max, nominal?, drift?}.
    Pass DEFAULT_CELL_DATA or a project-specific override dict.
    """
    cells: List[CellState] = []
    for r in range(racks):
        for m in range(modules_per_rack):
            for c in range(cells_per_module):
                cell = CellState(
                    project_id=project_id,
                    site_id=site_id,
                    rack_id=r,
                    module_id=m,
                    cell_id=c,
                )
                for name, cfg in cell_data.items():
                    lo      = float(cfg["min"])
                    hi      = float(cfg["max"])
                    nominal = float(cfg.get("nominal", (lo + hi) / 2))
                    drift   = float(cfg.get("drift", (hi - lo) * 0.005))
                    jitter  = random.uniform(-0.1, 0.1) * (hi - lo)
                    cell.measurements[name] = MeasurementState(
                        value   = max(lo, min(hi, nominal + jitter)),
                        min_val = lo,
                        max_val = hi,
                        drift   = random.uniform(-drift, drift),
                    )
                cells.append(cell)
    return cells
