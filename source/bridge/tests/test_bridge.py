#!/usr/bin/env python3
"""
test_bridge.py — unit tests for gen_bridge_conf.py

Covers:
  resolve_groups()      — group name → topic list resolution
  render_bridge_block() — dict → FlashMQ bridge {} config text

Run:
  python3 tests/test_bridge.py
"""

import io
import sys
import textwrap
import unittest
from pathlib import Path

# Make the parent directory importable regardless of working directory
sys.path.insert(0, str(Path(__file__).parent.parent))
from gen_bridge_conf import render_bridge_block, resolve_groups


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def lines(block: str) -> list[str]:
    """Return non-empty, stripped lines from a rendered block."""
    return [l.strip() for l in block.splitlines() if l.strip()]


def content_lines(block: str) -> list[str]:
    """Return non-comment, non-empty lines."""
    return [l.strip() for l in block.splitlines()
            if l.strip() and not l.strip().startswith("#")]


# ---------------------------------------------------------------------------
# resolve_groups
# ---------------------------------------------------------------------------

class TestResolveGroups(unittest.TestCase):

    def setUp(self):
        self.groups = {
            "batteries_all": {"topics": ["batteries/#"]},
            "solar_all":     {"topics": ["solar/#"]},
            "multi":         {"topics": ["wind/#", "hydro/#", "geo/#"]},
            "empty_group":   {"topics": []},
        }

    def test_single_group_single_topic(self):
        result = resolve_groups(["batteries_all"], self.groups, "b")
        self.assertEqual(result, ["batteries/#"])

    def test_multiple_groups_merged(self):
        result = resolve_groups(["batteries_all", "solar_all"], self.groups, "b")
        self.assertEqual(result, ["batteries/#", "solar/#"])

    def test_multi_topic_group_expanded(self):
        result = resolve_groups(["multi"], self.groups, "b")
        self.assertEqual(result, ["wind/#", "hydro/#", "geo/#"])

    def test_mixed_single_and_multi(self):
        result = resolve_groups(["batteries_all", "multi"], self.groups, "b")
        self.assertEqual(result, ["batteries/#", "wind/#", "hydro/#", "geo/#"])

    def test_empty_bridge_groups_list(self):
        result = resolve_groups([], self.groups, "b")
        self.assertEqual(result, [])

    def test_empty_topics_in_group(self):
        result = resolve_groups(["empty_group"], self.groups, "b")
        self.assertEqual(result, [])

    def test_unknown_group_skipped_with_warning(self):
        stderr = io.StringIO()
        with unittest.mock.patch("sys.stderr", stderr):
            result = resolve_groups(["batteries_all", "nonexistent"], self.groups, "mybridge")
        self.assertEqual(result, ["batteries/#"])
        self.assertIn("nonexistent", stderr.getvalue())
        self.assertIn("mybridge", stderr.getvalue())

    def test_duplicate_groups_both_included(self):
        # resolve_groups does not deduplicate — caller decides
        result = resolve_groups(["batteries_all", "batteries_all"], self.groups, "b")
        self.assertEqual(result, ["batteries/#", "batteries/#"])


# ---------------------------------------------------------------------------
# render_bridge_block
# ---------------------------------------------------------------------------

class TestRenderBridgeBlock(unittest.TestCase):

    def _minimal_conn(self, **extra):
        base = {"host": "flashmq.example.com", "port": 1883, "qos": 1}
        base.update(extra)
        return base

    # --- required fields ---

    def test_address_and_port_present(self):
        out = render_bridge_block("test", self._minimal_conn(), ["batteries/#"])
        self.assertIn("address flashmq.example.com", out)
        self.assertIn("port 1883", out)

    def test_block_opens_and_closes(self):
        out = render_bridge_block("test", self._minimal_conn(), ["batteries/#"])
        cl = content_lines(out)
        self.assertIn("bridge {", cl)
        self.assertEqual(cl[-1], "}")

    def test_publish_lines_for_each_topic(self):
        topics = ["batteries/#", "solar/#", "wind/#"]
        out = render_bridge_block("test", self._minimal_conn(), topics)
        for t in topics:
            self.assertIn(f"publish {t} 1", out)

    def test_topic_count_in_comment(self):
        topics = ["batteries/#", "solar/#"]
        conn = self._minimal_conn(groups=["g1", "g2"])
        out = render_bridge_block("test", conn, topics)
        self.assertIn("2 from 2 group(s)", out)

    def test_bridge_name_in_comment(self):
        out = render_bridge_block("aws_live", self._minimal_conn(), ["batteries/#"])
        self.assertIn("# Bridge: aws_live", out)

    def test_description_in_comment(self):
        conn = self._minimal_conn(description="Test bridge for CI")
        out = render_bridge_block("test", conn, ["batteries/#"])
        self.assertIn("# Test bridge for CI", out)

    # --- optional fields ---

    def test_protocol_version_emitted(self):
        conn = self._minimal_conn(protocol_version="mqtt3.1.1")
        out = render_bridge_block("test", conn, ["batteries/#"])
        self.assertIn("protocol_version mqtt3.1.1", out)

    def test_protocol_version_omitted_when_absent(self):
        out = render_bridge_block("test", self._minimal_conn(), ["batteries/#"])
        self.assertNotIn("protocol_version", out)

    def test_keepalive_emitted(self):
        conn = self._minimal_conn(keepalive=30)
        out = render_bridge_block("test", conn, ["batteries/#"])
        self.assertIn("keepalive 30", out)

    def test_clientid_prefix_emitted(self):
        conn = self._minimal_conn(clientid_prefix="site0-bridge")
        out = render_bridge_block("test", conn, ["batteries/#"])
        self.assertIn("clientid_prefix site0-bridge", out)

    def test_tls_off_not_emitted(self):
        # "off" and YAML boolean False both mean no tls line
        for tls_val in ("off", False):
            with self.subTest(tls=tls_val):
                conn = self._minimal_conn(tls=tls_val)
                out = render_bridge_block("test", conn, ["batteries/#"])
                self.assertNotIn("tls", out)

    def test_tls_on_emitted(self):
        conn = self._minimal_conn(tls="on")
        out = render_bridge_block("test", conn, ["batteries/#"])
        self.assertIn("tls on", out)

    def test_tls_unverified_emitted(self):
        conn = self._minimal_conn(tls="unverified")
        out = render_bridge_block("test", conn, ["batteries/#"])
        self.assertIn("tls unverified", out)

    def test_tls_yaml_true_maps_to_on(self):
        conn = self._minimal_conn(tls=True)
        out = render_bridge_block("test", conn, ["batteries/#"])
        self.assertIn("tls on", out)

    def test_ca_file_emitted_with_tls_on(self):
        conn = self._minimal_conn(tls="on", ca_file="/etc/ssl/ca.pem")
        out = render_bridge_block("test", conn, ["batteries/#"])
        self.assertIn("ca_file /etc/ssl/ca.pem", out)

    def test_ca_file_not_emitted_when_tls_off(self):
        conn = self._minimal_conn(tls="off", ca_file="/etc/ssl/ca.pem")
        out = render_bridge_block("test", conn, ["batteries/#"])
        self.assertNotIn("ca_file", out)

    def test_credentials_emitted(self):
        conn = self._minimal_conn(remote_username="user1", remote_password="s3cr3t")
        out = render_bridge_block("test", conn, ["batteries/#"])
        self.assertIn("remote_username user1", out)
        self.assertIn("remote_password s3cr3t", out)

    def test_credentials_omitted_when_absent(self):
        out = render_bridge_block("test", self._minimal_conn(), ["batteries/#"])
        self.assertNotIn("remote_username", out)
        self.assertNotIn("remote_password", out)

    def test_remote_clean_start_emitted(self):
        conn = self._minimal_conn(remote_clean_start=False)
        out = render_bridge_block("test", conn, ["batteries/#"])
        self.assertIn("remote_clean_start false", out)

    def test_qos_0_respected(self):
        conn = self._minimal_conn(qos=0)
        out = render_bridge_block("test", conn, ["batteries/#"])
        self.assertIn("publish batteries/# 0", out)

    def test_qos_2_respected(self):
        conn = self._minimal_conn(qos=2)
        out = render_bridge_block("test", conn, ["batteries/#", "solar/#"])
        self.assertIn("publish batteries/# 2", out)
        self.assertIn("publish solar/# 2", out)

    def test_wildcard_topics_preserved(self):
        topics = ["batteries/+/rack=0/#", "solar/#", "+/site=0/+/+"]
        out = render_bridge_block("test", self._minimal_conn(), topics)
        for t in topics:
            self.assertIn(t, out)

    def test_empty_topics_produces_no_publish_lines(self):
        out = render_bridge_block("test", self._minimal_conn(), [])
        self.assertNotIn("publish", out)


# ---------------------------------------------------------------------------
# Integration — round-trip through actual bridge.yaml
# ---------------------------------------------------------------------------

class TestRoundTrip(unittest.TestCase):

    def setUp(self):
        import yaml
        cfg_path = Path(__file__).parent.parent / "bridge.yaml"
        if not cfg_path.exists():
            self.skipTest("bridge.yaml not found")
        with open(cfg_path) as f:
            self.cfg = yaml.safe_load(f)

    def test_all_bridge_names_generate_output(self):
        groups  = self.cfg.get("groups", {})
        bridges = self.cfg.get("bridges", {})
        for name, conn in bridges.items():
            topics = resolve_groups(conn.get("groups", []), groups, name)
            if not topics:
                continue
            out = render_bridge_block(name, conn, topics)
            self.assertIn("bridge {", out)
            self.assertIn("address", out)
            self.assertTrue(out.strip().endswith("}"))

    def test_aws_live_bridge_contains_expected_topics(self):
        groups  = self.cfg.get("groups", {})
        bridges = self.cfg.get("bridges", {})
        if "aws_live" not in bridges:
            self.skipTest("aws_live bridge not defined")
        topics = resolve_groups(bridges["aws_live"].get("groups", []), groups, "aws_live")
        self.assertIn("batteries/#", topics)
        self.assertIn("solar/#", topics)

    def test_no_unknown_groups_in_bridges(self):
        groups  = self.cfg.get("groups", {})
        bridges = self.cfg.get("bridges", {})
        for bridge_name, conn in bridges.items():
            for group_name in conn.get("groups", []):
                self.assertIn(group_name, groups,
                    f"bridge '{bridge_name}' references undefined group '{group_name}'")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

import unittest.mock

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    for cls in [TestResolveGroups, TestRenderBridgeBlock, TestRoundTrip]:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    total  = result.testsRun
    failed = len(result.failures) + len(result.errors)
    print(f"\n{total - failed} passed, {failed} failed")
    sys.exit(0 if not failed else 1)
