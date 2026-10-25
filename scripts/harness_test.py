import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class RunIdentityTests(unittest.TestCase):
    def test_preparation_persists_and_renders_consistently(self):
        with tempfile.TemporaryDirectory() as temp:
            env = {**os.environ, "RUN_DIR": temp}
            script = ROOT / "scripts/00-prepare-run.sh"
            self.assertTrue(script.exists(), "shared run preparation is missing")
            def prepare(*args):
                subprocess.run(["bash", str(script), *args], env=env, check=True, capture_output=True)
                return (Path(temp) / "e2e-run-id").read_text().strip()
            first = prepare()
            self.assertEqual(first, prepare(), "sourcing scripts must not rotate identity")
            translation = json.loads((Path(temp) / "network/product-translation.json").read_text())
            demand = json.loads((Path(temp) / "network/demand.json").read_text())
            self.assertEqual(f"NET-SKU-{first}", translation["products"][0]["sku"])
            self.assertEqual(translation["products"][0]["networkProductId"], demand["demands"][0]["lines"][0]["networkProductId"])
            self.assertIn(first, demand["demands"][0]["networkRef"])
            self.assertNotIn("<run>", json.dumps([translation, demand]))
            second = prepare("--new")
            self.assertNotEqual(first, second)
            self.assertNotIn(first, (Path(temp) / "network/demand.json").read_text())

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
        self.assertIn('seed-process-paths.sh', source)
        self.assertLess(source.index('seed-process-paths.sh'), source.index('start_service wes '))
        self.assertNotIn('PATH_CATALOGUE_FILE=', source)
        self.assertEqual(4, source.count('PATH_CATALOGUE_SOURCE=kafka'))


class MCPRegistrationTests(unittest.TestCase):
    def test_all_eight_mcp_servers_are_built_started_and_stopped(self):
        build = (ROOT / "scripts/01-build.sh").read_text()
        up = (ROOT / "scripts/03-up-services.sh").read_text()
        down = (ROOT / "scripts/05-down-services.sh").read_text()
        env = (ROOT / "env.sh").read_text()
        self.assertIn('build_one process-path-mcp', build)
        self.assertIn('start_service process-path-mcp', up)
        self.assertIn('PROCESS_PATH_MCP_URL=', env)
        self.assertIn('process-path-mcp', down)
        self.assertNotRegex(env + up, r'MCP_(READ|READWRITE)_KEY|Authorization: Bearer')


class NetworkRegistrationTests(unittest.TestCase):
    def test_network_uses_dedicated_database_and_poll_seed(self):
        import yaml
        compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
        self.assertIn("postgres-network-fulfillment", compose["services"])
        database = compose["services"]["postgres-network-fulfillment"]
        self.assertIn("5449:5432", database["ports"])
        source = (ROOT / "scripts/03-up-services.sh").read_text()
        self.assertIn('NETWORK_MODE=stub', source)
        self.assertIn('GODOG_TAGS=@network-seed', source)
        self.assertNotIn('--seed-network', source)
        self.assertLess(source.index('GODOG_TAGS=@network-seed'), source.index('start_service network-fulfillment'))
        self.assertIn('NETWORK_SEED_FILE=', source)
        self.assertIn('PRODUCT_TRANSLATION_FILE=', source)
        self.assertIn('network-fulfillment', (ROOT / "scripts/05-down-services.sh").read_text())
        self.assertIn('netfulfil', (ROOT / "scripts/01-build.sh").read_text())
        self.assertNotRegex(source, r'(SP_API|NETWORK_TOKEN|NETWORK_API_KEY|AWS_SECRET)')


if __name__ == "__main__":
    unittest.main()
