#!/usr/bin/env python3
"""Check this checkout against the file listing of the run that produced the paper result.

``code-listing.json`` holds the unchanged entries (path, SHA-1) of the run listing for every file
in this release; ``PROVENANCE.json`` records the SHA-256 of the complete listing. Every code,
prompt and configuration file must be present here with the same bytes. Documentation that was
edited after the run to report its result is reported separately. No model, service or network
is used.
"""
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent


def is_documentation(name: str) -> bool:
    return name == "FROZEN-MANIFEST.json" or (name.endswith(".md") and not name.startswith("streamsqlagent/prompts/"))


provenance = json.loads((HERE / "PROVENANCE.json").read_text())
identical, documentation, problems = [], [], []
for name, digest in json.loads((HERE / "code-listing.json").read_text()):
    path = ROOT / name
    if path.is_file() and hashlib.sha1(path.read_bytes()).hexdigest() == digest:
        identical.append(name)
    elif path.is_file() and is_documentation(name):
        documentation.append(name)
    else:
        problems.append(name)
listing = provenance["run_listing"]
if len(identical) + len(documentation) + len(problems) + listing["not_in_this_release"] != listing["files"]:
    problems.append("code-listing.json does not account for the run listing")
print(json.dumps({"passed": not problems, "run_listing_files": listing["files"],
                  "identical": len(identical), "documentation_updated_after_run": documentation,
                  "not_in_this_release": listing["not_in_this_release"], "problems": problems}, indent=2))
raise SystemExit(bool(problems))
