#!/usr/bin/env python3
"""Compare coverage with committed deployment/source trees, never sibling worktrees."""
import argparse
import io
import json
import pathlib
import re
import subprocess
import sys
import tarfile

import hcl2
import yaml


def read_git_tree(repo, ref):
    raw = subprocess.check_output(["git", "-C", str(repo), "archive", ref])
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        return {m.name: archive.extractfile(m).read().decode("utf-8", errors="replace")
                for m in archive.getmembers() if m.isfile()}


def locals_from(source):
    result = {}
    for block in hcl2.loads(source).get("locals", []):
        result.update(block)
    return result


def literal_set(source, name):
    # Inspect the HCL AST: hcl2's dict transform discards quotes inside toset().
    for attr in hcl2.parses(source).find_data("attribute"):
        if str(attr.children[0].children[0]) != name:
            continue
        calls = list(attr.find_data("function_call"))
        tuples = list(attr.find_data("tuple"))
        if len(calls) != 1 or str(calls[0].children[0].children[0]) != "toset" or len(tuples) != 1:
            break
        values = []
        for expr in tuples[0].children:
            if getattr(expr, "data", None) == "new_line_or_comment":
                continue
            strings = list(expr.find_data("string"))
            if len(strings) != 1 or list(expr.find_data("interpolation")):
                raise ValueError(f"{name}: inventory must contain literal strings")
            values.append("".join(str(p.children[0]) for p in strings[0].children))
        return set(values)
    raise ValueError(f"{name}: expected a literal toset inventory")


def without_comments(source):
    # Keep strings intact, including URLs; comments cannot invent imports/routes.
    return re.sub(r'("(?:\\.|[^"\\])*"|\x27(?:\\.|[^\x27\\])*\x27)|//[^\n]*|/\*[\s\S]*?\*/',
                  lambda m: m.group(1) or "", source)


def check(manifest, trees):
    components = manifest["components"]
    infra = trees["warehouse-infra"]
    deployed = locals_from(infra["terraform/locals.tf"])
    core = set(deployed["services"])
    analytics = literal_set(infra["terraform/locals.tf"], "analytics_services")
    mcps = literal_set(infra["terraform/mcp.tf"], "mcp_services")
    backends = set(core)
    for file, key, owner in [("network-fulfillment.tf", "network_fulfillment_chart_path", "network-fulfillment"),
                              ("ops-agent.tf", "ops_agent_chart_path", "warehouse-ops-agent")]:
        if key in locals_from(infra["terraform/" + file]):
            backends.add(owner)
    expected = {name: {"backend", "oltp"} for name in core}
    for name in backends - core:
        expected[name] = {"backend"}
    for kind, names in [("mcp", mcps), ("projector", analytics), ("reports", analytics)]:
        for name in names:
            expected.setdefault(name, set()).add(kind)
    expected["warehouse-console"] = {"shell"}
    app = without_comments(trees["warehouse-console"]["src/App.tsx"])
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
        tree = trees.get(name, {})
        # cmd directories with Go source are independent executables in this fleet.
        actual = {p.split('/')[1] for p in tree if re.match(r'^cmd/[^/]+/[^/]+\.go$', p) and not p.endswith('_test.go')}
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
    names = set(manifest["components"]) | {"warehouse-infra", "warehouse-console"}
    # Infrastructure owns the set of backend repositories, even if a manifest row is deleted.
    infra = read_git_tree(args.repos_root / "warehouse-infra", refs["warehouse-infra"] if refs else "origin/develop")
    names.update(locals_from(infra["terraform/locals.tf"])["services"])
    names.update(["network-fulfillment", "warehouse-ops-agent"])
    trees = {}
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
