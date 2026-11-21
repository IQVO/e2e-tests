import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class RunIdentityTests(unittest.TestCase):
    def test_preparation_persists_consistently(self):
        with tempfile.TemporaryDirectory() as temp:
            env = {**os.environ, "RUN_DIR": temp}
            script = ROOT / "scripts/00-prepare-run.sh"
            self.assertTrue(script.exists(), "shared run preparation is missing")
            def prepare(*args):
                subprocess.run(["bash", str(script), *args], env=env, check=True, capture_output=True)
                return (Path(temp) / "e2e-run-id").read_text().strip()
            first = prepare()
            self.assertRegex(first, r"^[A-Za-z0-9]+$")
            self.assertEqual(first, prepare(), "sourcing scripts must not rotate identity")
            second = prepare("--new")
            self.assertNotEqual(first, second)

    def test_live_process_prevents_identity_rotation(self):
        with tempfile.TemporaryDirectory() as temp:
            env = {**os.environ, "RUN_DIR": temp}
            script = ROOT / "scripts/00-prepare-run.sh"
            self.assertTrue(script.exists(), "shared run preparation is missing")
            subprocess.run(["bash", str(script)], env=env, check=True, capture_output=True)
            pids = Path(temp) / "pids"
            pids.mkdir(exist_ok=True)
            (pids / "fixture.pid").write_text(str(os.getpid()))
            result = subprocess.run(["bash", str(script), "--new"], env=env, capture_output=True, text=True)
            self.assertNotEqual(0, result.returncode)
            self.assertIn("running", result.stderr)


class LiveCatalogueWiringTests(unittest.TestCase):
    def test_catalogue_is_seeded_before_kafka_consumers_start(self):
        source = (ROOT / "scripts/03-up-services.sh").read_text()
        self.assertIn("seed-process-paths.sh", source)
        self.assertLess(source.index("seed-process-paths.sh"), source.index("start_service wes "))
        # The four HTTP consumers replay the catalogue from Kafka, not a file.
        self.assertEqual(4, source.count("PATH_CATALOGUE_SOURCE=kafka"))
        # File source survives only inside the workforce MCP server's block.
        self.assertEqual(1, source.count("PATH_CATALOGUE_FILE="))
        self.assertGreater(source.index("PATH_CATALOGUE_FILE="),
                           source.index("start_service workforce-mcp "))


class MCPRegistrationTests(unittest.TestCase):
    def test_every_mcp_binary_is_built_started_and_stopped(self):
        build = (ROOT / "scripts/01-build.sh").read_text()
        up = (ROOT / "scripts/03-up-services.sh").read_text()
        down = (ROOT / "scripts/05-down-services.sh").read_text()
        env = (ROOT / "env.sh").read_text()
        built = set(re.findall(r"build_one (\S+)-mcp ", build))
        self.assertTrue(built, "no MCP binaries found in 01-build.sh")
        for name in sorted(built):
            starter = f"start_service {name}-mcp " if name != "execution" \
                else f"start_service_in execution-mcp "
            self.assertIn(starter, up,
                          f"{name}-mcp is built but never started")
            self.assertIn(f"{name}-mcp", down,
                          f"{name}-mcp is built but never stopped")
        self.assertNotRegex(env + up, r"MCP_(READ|READWRITE)_KEY|static.bearer")


class NetworkRegistrationTests(unittest.TestCase):
    def test_network_uses_dedicated_database_and_poll_seed(self):
        import yaml
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
        self.assertIn("postgres-network", compose["services"])
        database = compose["services"]["postgres-network"]
        self.assertIn("5449:5432", database["ports"])
        source = (ROOT / "scripts/03-up-services.sh").read_text()
        self.assertIn("start_service network ", source)
        # Inbound demand arrives only through the seeded poller file, never
        # real network credentials.
        self.assertIn("NETWORK_SEED_FILE=", source)
        self.assertIn("PRODUCT_TRANSLATION_FILE=", source)
        self.assertIn("network", (ROOT / "scripts/05-down-services.sh").read_text())
        self.assertIn("netfulfil", (ROOT / "scripts/01-build.sh").read_text())
        self.assertNotRegex(source, r"(SP_API|NETWORK_TOKEN|NETWORK_API_KEY|AWS_SECRET)")


if __name__ == "__main__":
    unittest.main()
