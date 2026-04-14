"""
gen_point_registry.py — generate point_registry.json from ems_topic_template.json

Assigns a 4-hex-digit ID to every unique point name in the template.
IDs are assigned alphabetically for determinism — adding new points appends
to the end without renumbering existing entries.

Usage:
    python3 tools/gen_point_registry.py \
        --template source/stress_runner/ems_topic_template.json \
        --output   source/stress_runner/point_registry.json

Output format (id → name, for decoding at writer/subscriber side):
    {
      "version": 1,
      "description": "EMS point name registry — 4-hex-char IDs",
      "points": {
        "0000": "ACBreaker",
        "0001": "AphA",
        ...
      }
    }
"""

import argparse
import json
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", default="source/stress_runner/ems_topic_template.json")
    parser.add_argument("--output",   default="source/stress_runner/point_registry.json")
    args = parser.parse_args()

    with open(args.template) as f:
        template = json.load(f)

    # Collect all unique point names, sorted for deterministic ID assignment
    unique_names = sorted(set(entry[2] for entry in template["template"]))

    if len(unique_names) > 0xFFFF:
        print(f"ERROR: {len(unique_names)} unique names exceeds 4-hex-digit space (65535)",
              file=sys.stderr)
        sys.exit(1)

    # id (hex str) → name
    points = {f"{i:04X}": name for i, name in enumerate(unique_names)}

    registry = {
        "version": 1,
        "description": "EMS point name registry — 4-hex-char IDs mapped to full point names",
        "count": len(points),
        "points": points,
    }

    with open(args.output, "w") as f:
        json.dump(registry, f, indent=2)

    print(f"Written {len(points)} point IDs → {args.output}")

    # Also print a reverse mapping summary
    avg_len = sum(len(n) for n in unique_names) / len(unique_names)
    max_len = max(len(n) for n in unique_names)
    print(f"Point name lengths: avg={avg_len:.0f}  max={max_len}")
    print(f"Topic saving: avg ~{avg_len - 4:.0f} chars/msg  "
          f"({avg_len:.0f}B point name → 4B hex ID)")


if __name__ == "__main__":
    main()
