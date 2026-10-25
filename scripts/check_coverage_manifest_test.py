"""Contract inventory fixtures are deliberately independent of working trees."""
import copy
import importlib.util
import pathlib
import unittest

SPEC = importlib.util.spec_from_file_location("coverage_check", pathlib.Path(__file__).with_name("check-coverage-manifest.py"))


class CoverageManifestTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(pathlib.Path(SPEC.origin).exists(), "coverage drift checker is missing")
        self.checker = importlib.util.module_from_spec(SPEC)
        SPEC.loader.exec_module(self.checker)
        import json
        fixture = pathlib.Path(__file__).parent / "fixtures" / "coverage-tree.json"
        self.tree = json.loads(fixture.read_text())
        self.manifest = copy.deepcopy(self.tree.pop("manifest"))

    def check(self):
        return self.checker.check(self.manifest, self.tree)

    def test_complete_fixture(self):
        self.assertEqual([], self.check())

    def test_missing_surfaces_name_owner(self):
        for owner, kinds in [("network-fulfillment", ["backend"]), ("alpha", ["mcp"]), ("alpha", ["projector", "reports"]), ("alpha", ["mfe"])]:
            with self.subTest(owner=owner, kinds=kinds):
                old = self.manifest["components"][owner]["kinds"][:]
                self.manifest["components"][owner]["kinds"] = [k for k in old if k not in kinds]
                errors = self.check()
                self.assertTrue(any(owner in e and kinds[0] in e for e in errors), errors)
                self.manifest["components"][owner]["kinds"] = old

    def test_phantom_runtime(self):
        self.manifest["components"]["ghost"] = {"kinds": ["mcp"], "nativeScenarios": [], "clusterChecks": []}
        self.assertTrue(any("ghost" in e for e in self.check()))

    def test_removed_binary(self):
        del self.tree["alpha"]["cmd/mcp/main.go"]
        self.assertTrue(any("alpha" in e and "mcp" in e for e in self.check()))

    def test_new_deployable_binary(self):
        self.tree["alpha"]["cmd/unlisted-worker/main.go"] = "package main"
        self.assertTrue(any("unlisted-worker" in e for e in self.check()))

    def test_commented_inventory_is_not_runtime(self):
        self.tree["warehouse-infra"]["terraform/locals.tf"] += '\n# "ghost" = {}\n'
        self.tree["warehouse-console"]["src/App.tsx"] += '\n// import("ghost_mfe/App")\n'
        self.assertEqual([], self.check())

    def test_import_requires_live_route(self):
        self.tree["warehouse-console"]["src/App.tsx"] = 'const Alpha = lazy(() => import("alpha_mfe/App"));'
        self.assertTrue(any("alpha" in e and "route" in e for e in self.check()))

    def test_committed_tree_ignores_working_copy(self):
        import subprocess
        import tempfile
        with tempfile.TemporaryDirectory() as temp:
            def git(*args):
                return subprocess.check_output(["git", "-C", temp, *args], text=True)
            git("init", "-q")
            path = pathlib.Path(temp) / "contract.txt"
            path.write_text("committed")
            git("add", ".")
            git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")
            git("update-ref", "refs/remotes/origin/develop", "HEAD")
            path.write_text("dirty")
            tree = self.checker.read_git_tree(pathlib.Path(temp), "origin/develop")
            self.assertEqual("committed", tree["contract.txt"])


if __name__ == "__main__":
    unittest.main()
