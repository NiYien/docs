#!/usr/bin/env python3
"""Generate cameras/cameras.json (supported-camera list) from the camera_db.

Data source is the canonical niyien-lens-data repository (local clone).
Run from anywhere; paths are resolved relative to this script:

    python scripts/generate_supported_cameras.py [--db <path-to-camera_db>]

Workflow: whenever camera_db changes, re-run this script and commit the
regenerated cameras/cameras.json together with any page changes, then push
the docs repo to trigger the Vercel rebuild.

The output JSON is a static snapshot consumed by cameras/index.html:

    { "generated_at": "YYYY-MM-DD",
      "brands": [ { "id": "...", "name": "...", "models": ["...", ...] } ] }

Model order follows the `models` object order in each vendor JSON (the data
repo controls display order). Keys not present in DISPLAY_NAMES are emitted
verbatim, so vendors whose keys are already market names need no mapping.
"""

import argparse
import datetime
import json
import sys
from pathlib import Path

# Brand order defines the display order on the page.
# (file stem, display name) — every camera_db/*.json must be listed here.
BRANDS = [
    ("sony", "Sony"),
    ("canon", "Canon"),
    ("nikon", "Nikon"),
    ("fujifilm", "Fujifilm"),
    ("lumix", "Panasonic LUMIX"),
    ("blackmagic", "Blackmagic Design"),
    ("red", "RED"),
    ("kinefinity", "Kinefinity"),
    ("leica", "Leica"),
    ("sigma", "Sigma"),
    ("ricoh", "Ricoh"),
    ("zcam", "Z CAM"),
]

# camera_db key -> market display name. Unlisted keys pass through as-is.
DISPLAY_NAMES = {
    "sony": {
        "ILCE-1M2":  "α1 II",
        "ILCE-1":    "α1",
        "ILCE-9M3":  "α9 III",
        "ILCE-9M2":  "α9 II",
        "ILCE-9":    "α9",
        "ILCE-7SM3": "α7S III",
        "ILCE-7SM2": "α7S II",
        "ILCE-7S":   "α7S",
        "ILCE-7RM5": "α7R V",
        "ILCE-7RM4": "α7R IV",
        "ILCE-7RM3": "α7R III",
        "ILCE-7RM2": "α7R II",
        "ILCE-7M4":  "α7 IV",
        "ILCE-7M3":  "α7 III",
        "ILCE-7M2":  "α7 II",
        "ILCE-7":    "α7",
        "ILCE-7CM2": "α7C II",
        "ILCE-7CR":  "α7CR",
        "ILCE-7C":   "α7C",
        "ILCE-FX2":  "FX2",
        "ILCE-FX30": "FX30",
        "ILCE-FX3":  "FX3",
        "ILCE-FX6":  "FX6",
        "ILCE-6700": "α6700",
        "ILCE-6600": "α6600",
        "ILCE-6500": "α6500",
        "ILCE-6400": "α6400",
        "ILCE-6300": "α6300",
        "ILCE-6100": "α6100",
        "ZVE1":      "ZV-E1",
        "ZVE10M2":   "ZV-E10 II",
        "ZVE10":     "ZV-E10",
        "ZV1":       "ZV-1",
        "RX100M7":   "RX100 VII",
        "RX100VA":   "RX100 VA",
        "RX1RM2":    "RX1R II",
        "RX10M4":    "RX10 IV",
    },
    "lumix": {
        "S1M2":    "S1 II",
        "S1R2":    "S1R II",
        "S5M2X":   "S5 IIX",
        "S5M2":    "S5 II",
        "G9M2":    "G9 II",
        "GH5M2":   "GH5 II",
        "LX100M2": "LX100 II",
    },
    "leica": {
        "240P": "M (Typ 240)",
    },
}


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=str(repo_root.parent / "niyien-lens-data" / "camera_db"),
        help="Path to the camera_db directory (default: ../niyien-lens-data/camera_db)",
    )
    args = parser.parse_args()

    db_dir = Path(args.db)
    if not db_dir.is_dir():
        print(f"error: camera_db directory not found: {db_dir}", file=sys.stderr)
        return 1

    # Bidirectional check: every vendor file must be registered in BRANDS and
    # vice versa, so a new vendor JSON can't silently miss the page.
    disk_stems = {p.stem for p in db_dir.glob("*.json")}
    brand_stems = {stem for stem, _ in BRANDS}
    unregistered = sorted(disk_stems - brand_stems)
    missing = sorted(brand_stems - disk_stems)
    if unregistered:
        print(f"error: vendor file(s) not registered in BRANDS: {', '.join(unregistered)}", file=sys.stderr)
    if missing:
        print(f"error: BRANDS entry has no vendor file: {', '.join(missing)}", file=sys.stderr)
    if unregistered or missing:
        return 1

    brands_out = []
    for stem, display in BRANDS:
        with open(db_dir / f"{stem}.json", encoding="utf-8") as f:
            data = json.load(f)
        mapping = DISPLAY_NAMES.get(stem, {})
        models = [mapping.get(key, key) for key in data.get("models", {}).keys()]
        if not models:
            print(f"error: {stem}.json has an empty models section", file=sys.stderr)
            return 1
        brands_out.append({"id": stem, "name": display, "models": models})

    out = {
        "generated_at": datetime.date.today().isoformat(),
        "brands": brands_out,
    }

    out_path = repo_root / "cameras" / "cameras.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
        f.write("\n")

    total = sum(len(b["models"]) for b in brands_out)
    print(f"wrote {out_path} ({len(brands_out)} brands, {total} models)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
