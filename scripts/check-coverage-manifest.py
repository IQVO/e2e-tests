#!/usr/bin/env python3
"""Compare coverage with committed deployment/source trees, never sibling worktrees.

The deployed truth is warehouse-infra's application registry
(terraform/apps.yaml, ADR 0007): one entry per application, with the
flags that decide which surfaces exist (kind, database, analytics,
frontend_remote, site). Binary names are verified against each repo's
own committed cmd/ tree at origin/develop (or an exact-SHA snapshot via
--refs, as the cluster-acceptance release gate does).
"""
import argparse
import io
import json
import pathlib
import re
import subprocess
import sys
import tarfile

import yaml


def read_git_tree(repo, ref):
    raw = subprocess.check_output(["git", "-C", str(repo), "archive", ref])
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        return {m.name: archive.extractfile(m).read().decode("utf-8", errors="replace")
                for m in archive.getmembers() if m.isfile()}


def registry_from(infra):
    apps = yaml.safe_load(infra["terraform/apps.yaml"]).get("applications")
    if not apps:
        raise ValueError("terraform/apps.yaml: no applications registry")
    return apps


def cmd_dirs(tree):
    # cmd directories with Go source are independent executables in this fleet.
    return {p.split("/")[1] for p in tree if re.match(r"^cmd/[^/]+/[^/]+\.go$", p) and not p.endswith("_test.go")}


def without_comments(source):
    # Keep strings intact, including URLs; comments cannot invent imports/routes.
    return re.sub(r'("(?:\\.|[^"\\])*"|\x27(?:\\.|[^\x27\\])*\x27)|//[^\n]*|/\*[\s\S]*?\*/',
                  lambda m: m.group(1) or "", source)


def expected_kinds(app, cmds):
    kinds = set()
    if cmds:
        # Every backend application in this fleet ships its runtime as
        # cmd/<name> binaries (registry-image or not); sites ship none.
        kinds.add("backend")
    if app.get("kind") == "service" and app.get("database"):
        kinds.add("oltp")
    if app.get("analytics"):
        kinds.update(("projector", "reports"))
    if "mcp" in cmds:
        kinds.add("mcp")
    if app.get("frontend_remote"):
        kinds.add("mfe")
    if app.get("site"):
        kinds.add("shell")
    return kinds


def check(manifest, trees):
    components = manifest["components"]
    infra = trees["warehouse-infra"]
    registry = registry_from(infra)
    console_name = "warehouse-console"
    expected = {}
    backends = set()
    for name, app in registry.items():
        cmds = cmd_dirs(trees.get(name, {}))
        kinds = expected_kinds(app, cmds)
        if not kinds and name not in components:
            continue
        expected[name] = kinds
        if "backend" in kinds:
            backends.add(name)
    expected.setdefault(console_name, {"shell"})
    app = without_comments(trees[console_name]["src/App.tsx"])
    imports = dict(re.findall(r'const\s+(\w+)\s*=\s*lazy\(\(\)\s*=>\s*import\(["\x27]([^"\x27]+)["\x27]\)', app))
    errors = []
    for symbol, remote in imports.items():
        owners = [name for name, row in components.items() if row.get("remote") == remote]
        if len(owners) != 1:
            errors.append(f"unowned or ambiguous mfe import: {remote}")
            continue
        name = owners[0]
        expected.setdefault(name, set()).add("mfe")
        if not re.search(r'<Route\b[^>]*\bpath=["\x27][^"\x27]+["\x27][\s\S]*?component=\{' + re.escape(symbol) + r'\}', app):
            errors.append(f"{name}: mfe import has no live route")
    for name in sorted(set(components) | set(expected)):
        declared = set(components.get(name, {}).get("kinds", []))
        actual = expected.get(name, set())
        for kind in sorted(actual - declared):
            errors.append(f"{name}: missing {kind} coverage")
        for kind in sorted(declared - actual):
            errors.append(f"{name}: phantom {kind} coverage")
    for name in sorted(backends):
        row = components.get(name, {})
        binaries = row.get("commands", {})
        actual = cmd_dirs(trees.get(name, {}))
        for cmd in sorted(actual - set(binaries.values())):
            errors.append(f"{name}: unlisted cmd/{cmd} runtime")
        for kind, cmd in binaries.items():
            if cmd not in actual:
                errors.append(f"{name}: missing {kind} binary cmd/{cmd}")
        for kind in expected.get(name, set()) & {"backend", "mcp", "projector", "reports"}:
            if kind not in binaries:
                errors.append(f"{name}: missing {kind} command mapping")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repos-root", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[2])
    parser.add_argument("--manifest", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1] / "coverage.yaml")
    parser.add_argument("--refs", type=pathlib.Path, help="JSON repo -> exact SHA (CI resolved snapshot)")
    args = parser.parse_args()
    manifest = yaml.safe_load(args.manifest.read_text())
    refs = json.loads(args.refs.read_text()) if args.refs else None
    infra = read_git_tree(args.repos_root / "warehouse-infra", refs["warehouse-infra"] if refs else "origin/develop")
    names = set(manifest["components"]) | set(registry_from(infra)) | {"warehouse-console"}
    trees = {"warehouse-infra": infra}
    for name in sorted(names):
        ref = refs[name] if refs else "origin/develop"
        if refs and not re.fullmatch(r"[0-9a-f]{40}", ref):
            raise ValueError(f"{name}: expected exact SHA, got {ref!r}")
        trees[name] = read_git_tree(args.repos_root / name, ref)
    errors = check(manifest, trees)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    counts = {kind: sum(kind in r["kinds"] for r in manifest["components"].values())
              for kind in ["backend", "mcp", "projector", "reports", "mfe", "shell"]}
    print("coverage inventory: " + ", ".join(f"{n} {k}" for k, n in counts.items()))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyError, ValueError, OSError, subprocess.CalledProcessError) as exc:
        sys.exit(f"coverage inventory could not be verified: {exc}")
