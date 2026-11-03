import sys
import os
import unittest
from unittest.mock import patch, MagicMock
import pytest

# Ensure dags directory is on sys.path
dags_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dags"))
if dags_dir not in sys.path:
    sys.path.insert(0, dags_dir)

import create_solr_collection_cli as ccli
import update_collection_alias_cli as ucli


class TestSolrReplicaSync:

    @pytest.fixture(autouse=True)
    def setup_sample_cluster_status(self):
        self.solr_url = "http://localhost:8983/solr"
        self.collection = "biocache-2026-03-11"
        self.live_nodes = ["10.0.0.1:8983_solr", "10.0.0.2:8983_solr"]

        self.healthy_cluster_status = {
            "cluster": {
                "live_nodes": self.live_nodes,
                "collections": {
                    self.collection: {
                        "shards": {
                            "shard1": {
                                "state": "active",
                                "replicas": {
                                    "core_node1": {
                                        "state": "active",
                                        "node_name": "10.0.0.1:8983_solr",
                                        "leader": "true",
                                    },
                                    "core_node2": {
                                        "state": "active",
                                        "node_name": "10.0.0.2:8983_solr",
                                    },
                                },
                            },
                            "shard2": {
                                "state": "active",
                                "replicas": {
                                    "core_node3": {
                                        "state": "active",
                                        "node_name": "10.0.0.2:8983_solr",
                                        "leader": "true",
                                    },
                                    "core_node4": {
                                        "state": "active",
                                        "node_name": "10.0.0.1:8983_solr",
                                    },
                                },
                            },
                        }
                    }
                },
            }
        }

    # 1. wait_for_replicas_active: all active immediately
    @patch("create_solr_collection_cli.fetch_data")
    def test_wait_for_replicas_active_all_active(self, mock_fetch):
        mock_fetch.return_value = self.healthy_cluster_status

        result = ccli.wait_for_replicas_active(
            self.collection, self.solr_url, expected_rf=2, timeout_seconds=10, poll_interval=0.1
        )
        assert result is True
        assert mock_fetch.call_count == 1

    # 2. wait_for_replicas_active: recovering then active (retry loop)
    @patch("time.sleep")
    @patch("create_solr_collection_cli.fetch_data")
    def test_wait_for_replicas_active_recovering_then_active(self, mock_fetch, mock_sleep):
        import copy

        recovering_status = copy.deepcopy(self.healthy_cluster_status)
        recovering_status["cluster"]["collections"][self.collection]["shards"]["shard1"]["replicas"]["core_node2"][
            "state"
        ] = "recovering"

        mock_fetch.side_effect = [recovering_status, recovering_status, self.healthy_cluster_status]

        result = ccli.wait_for_replicas_active(
            self.collection, self.solr_url, expected_rf=2, timeout_seconds=10, poll_interval=0.1
        )
        assert result is True
        assert mock_fetch.call_count == 3
        assert mock_sleep.call_count == 2

    # 3. wait_for_replicas_active: missing replica / expected rf not met
    @patch("time.time")
    @patch("time.sleep")
    @patch("create_solr_collection_cli.fetch_data")
    def test_wait_for_replicas_active_missing_replica(self, mock_fetch, mock_sleep, mock_time):
        import copy

        missing_rep_status = copy.deepcopy(self.healthy_cluster_status)
        del missing_rep_status["cluster"]["collections"][self.collection]["shards"]["shard1"]["replicas"]["core_node2"]

        mock_fetch.return_value = missing_rep_status
        # Simulate time progression past timeout
        mock_time.side_effect = [0, 5, 15]

        with pytest.raises(TimeoutError, match="Timed out after 10s"):
            ccli.wait_for_replicas_active(
                self.collection, self.solr_url, expected_rf=2, timeout_seconds=10, poll_interval=1
            )

    # 4. wait_for_replicas_active: node hosting replica is not in live_nodes
    @patch("time.time")
    @patch("time.sleep")
    @patch("create_solr_collection_cli.fetch_data")
    def test_wait_for_replicas_active_node_not_in_live_nodes(self, mock_fetch, mock_sleep, mock_time):
        import copy

        dead_node_status = copy.deepcopy(self.healthy_cluster_status)
        dead_node_status["cluster"]["live_nodes"] = ["10.0.0.1:8983_solr"]  # 10.0.0.2 is dead

        mock_fetch.return_value = dead_node_status
        mock_time.side_effect = [0, 5, 15]

        with pytest.raises(TimeoutError, match="Timed out after 10s"):
            ccli.wait_for_replicas_active(
                self.collection, self.solr_url, expected_rf=2, timeout_seconds=10, poll_interval=1
            )

    # 5. wait_for_replicas_active: HTTP error / transient failure then recover
    @patch("time.sleep")
    @patch("create_solr_collection_cli.fetch_data")
    def test_wait_for_replicas_active_http_error_recovery(self, mock_fetch, mock_sleep):
        mock_fetch.side_effect = [
            RuntimeError("Solr connection refused"),
            self.healthy_cluster_status,
        ]

        result = ccli.wait_for_replicas_active(
            self.collection, self.solr_url, expected_rf=2, timeout_seconds=10, poll_interval=0.1
        )
        assert result is True
        assert mock_fetch.call_count == 2

    # 6. add_additional_replicas: with wait=True
    @patch("create_solr_collection_cli.wait_for_replicas_active")
    @patch("create_solr_collection_cli.fetch_data")
    @patch("create_solr_collection_cli.get_live_nodes")
    def test_add_additional_replicas_with_wait(self, mock_get_nodes, mock_fetch, mock_wait):
        mock_get_nodes.return_value = ["node1", "node2"]

        ccli.add_additional_replicas(
            self.collection, self.solr_url, rf=2, wait=True, timeout_seconds=600, poll_interval=5
        )
        # Should have called fetch_data for ADDREPLICA and then wait_for_replicas_active
        mock_wait.assert_called_once_with(
            collection=self.collection,
            solr_url=self.solr_url,
            expected_rf=2,
            timeout_seconds=600,
            poll_interval=5,
        )

    # 7. add_additional_replicas: with wait=False
    @patch("create_solr_collection_cli.wait_for_replicas_active")
    @patch("create_solr_collection_cli.fetch_data")
    @patch("create_solr_collection_cli.get_live_nodes")
    def test_add_additional_replicas_no_wait(self, mock_get_nodes, mock_fetch, mock_wait):
        mock_get_nodes.return_value = ["node1", "node2"]

        ccli.add_additional_replicas(self.collection, self.solr_url, rf=2, wait=False)
        mock_wait.assert_not_called()

    # 8. check_replicas_active in update_collection_alias_cli: PASS condition
    @patch("update_collection_alias_cli.json_parse")
    def test_check_replicas_active_pass(self, mock_json_parse):
        mock_json_parse.return_value = self.healthy_cluster_status
        ucli.solr_base = self.solr_url
        ucli.solr_base_new = None
        ucli.new_collection = self.collection

        status = ucli.check_replicas_active()
        assert status == ucli.ResultStatus.PASS

    # 9. check_replicas_active: FAIL condition when replica recovering
    @patch("update_collection_alias_cli.json_parse")
    def test_check_replicas_active_fail_recovering(self, mock_json_parse):
        import copy

        recovering_status = copy.deepcopy(self.healthy_cluster_status)
        recovering_status["cluster"]["collections"][self.collection]["shards"]["shard1"]["replicas"]["core_node2"][
            "state"
        ] = "recovering"
        mock_json_parse.return_value = recovering_status
        ucli.solr_base = self.solr_url
        ucli.solr_base_new = None
        ucli.new_collection = self.collection

        status = ucli.check_replicas_active()
        assert status == ucli.ResultStatus.FAIL

    # 10. check_replicas_active: FAIL condition when shard is down
    @patch("update_collection_alias_cli.json_parse")
    def test_check_replicas_active_fail_shard_down(self, mock_json_parse):
        import copy

        shard_down_status = copy.deepcopy(self.healthy_cluster_status)
        shard_down_status["cluster"]["collections"][self.collection]["shards"]["shard1"]["state"] = "inactive"
        mock_json_parse.return_value = shard_down_status
        ucli.solr_base = self.solr_url
        ucli.solr_base_new = None
        ucli.new_collection = self.collection

        status = ucli.check_replicas_active()
        assert status == ucli.ResultStatus.FAIL

    # 11. switch_collection_alias: aborts when replicas are not active
    @patch("update_collection_alias_cli.check_replicas_active")
    @patch("update_collection_alias_cli.json_parse")
    def test_switch_collection_alias_aborts_on_inactive_replicas(self, mock_json_parse, mock_check_replicas):
        mock_check_replicas.return_value = ucli.ResultStatus.FAIL
        ucli.run_checks_only = False
        ucli.solr_base = self.solr_url
        ucli.solr_base_new = None
        ucli.new_collection = self.collection
        ucli.solr_alias = "biocache"

        status = ucli.switch_collection_alias()
        assert status == ucli.ResultStatus.FAIL
        # Verify REBALANCELEADERS and CREATEALIAS were never called
        mock_json_parse.assert_not_called()

    # 12. switch_collection_alias: proceeds when replicas are active
    @patch("update_collection_alias_cli.check_replicas_active")
    @patch("update_collection_alias_cli.json_parse")
    def test_switch_collection_alias_proceeds_when_replicas_active(self, mock_json_parse, mock_check_replicas):
        mock_check_replicas.return_value = ucli.ResultStatus.PASS
        mock_json_parse.side_effect = [
            {"responseHeader": {"status": 0}},  # REBALANCELEADERS
            {"responseHeader": {"status": 0}},  # CREATEALIAS
        ]
        ucli.run_checks_only = False
        ucli.solr_base = self.solr_url
        ucli.solr_base_new = None
        ucli.new_collection = self.collection
        ucli.solr_alias = "biocache"

        status = ucli.switch_collection_alias()
        assert status == ucli.ResultStatus.PASS
        assert mock_json_parse.call_count == 2
