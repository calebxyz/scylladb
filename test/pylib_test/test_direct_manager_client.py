#!/usr/bin/env python3
# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from test.pylib.direct_manager_client import DirectManagerClient
from test.pylib.internal_types import HostID, IPAddress, ServerNum, ServerUpState
from test.pylib.scylla_cluster import ScyllaClusterManager


class FakeServer:
    def __init__(self, ip_addr: str, host_id: str, log_filename: str) -> None:
        self.ip_addr = IPAddress(ip_addr)
        self._host_id = HostID(host_id)
        self.log_filename = log_filename
        self.workdir = Path("/tmp/workdir")
        self.maintenance_socket_path = Path("/tmp/maintenance")
        self.exe = Path("/tmp/scylla")
        self.cmd = None

    async def get_host_id(self, _api) -> HostID:
        return self._host_id


class FakeCluster:
    def __init__(self) -> None:
        self.is_dirty = False
        self.is_running = True
        self.replicas = 3
        self.keyspace_count = 2
        self.running = {
            ServerNum(1): FakeServer("127.0.0.1", "host-1", "/tmp/server-1.log"),
            ServerNum(2): FakeServer("127.0.0.2", "host-2", "/tmp/server-2.log"),
        }
        self.stopped = {}
        self.starting = {}
        self.removed = set()
        self.servers = {**self.running}
        self.api_calls = []
        self.server_start_calls = []
        self.before_tests = []
        self.after_tests = []
        self.logger = None
        self.api = SimpleNamespace(remove_node=self._remove_node)

    def __str__(self) -> str:
        return "FakeCluster"

    async def _remove_node(self, initiator_ip, host_id, ignore_dead, timeout) -> None:
        self.api_calls.append((initiator_ip, host_id, ignore_dead, timeout))

    def setLogger(self, logger) -> None:
        self.logger = logger

    def before_test(self, name: str) -> None:
        self.before_tests.append(name)

    def after_test(self, name: str, success: bool) -> None:
        self.after_tests.append((name, success))

    def take_log_savepoint(self) -> None:
        self.savepoint_taken = True

    def _get_keyspace_count(self) -> int:
        return 7

    async def server_start(self, **kwargs) -> None:
        self.server_start_calls.append(kwargs)

    def server_mark_removed(self, server_id: ServerNum) -> None:
        self.removed.add(server_id)


def make_manager(tmp_path: Path, cluster: FakeCluster) -> ScyllaClusterManager:
    manager = object.__new__(ScyllaClusterManager)
    manager.test_uname = "test/cluster/test_topology"
    manager.base_dir = str(tmp_path)
    manager.logger = logging.getLogger("test.manager")
    manager.current_test_case_full_name = ""
    manager.cluster = cluster
    manager.site = None
    manager.clusters = None
    manager.is_running = True
    manager.is_before_test_ok = False
    manager.is_after_test_ok = False
    manager.tasks_history = {}
    manager.server_broken_event = asyncio.Event()
    manager.server_broken_reason = ""
    return manager


@pytest.mark.asyncio
async def test_direct_manager_client_before_and_after_test(tmp_path: Path) -> None:
    cluster = FakeCluster()
    manager = make_manager(tmp_path, cluster)
    client = DirectManagerClient(manager)

    cluster_name = await client.put_json(
        "/cluster/before-test/test_case", response_type="json"
    )

    assert cluster_name == "FakeCluster"
    assert cluster.before_tests == ["test/cluster/test_topology::test_case"]

    result = await client.put_json("/cluster/after-test/True", response_type="json")

    assert result["cluster_str"] == "FakeCluster"
    assert result["server_broken"] is False
    assert cluster.after_tests == [("test/cluster/test_topology::test_case", True)]


@pytest.mark.asyncio
async def test_direct_manager_client_start_and_remove_node(tmp_path: Path) -> None:
    cluster = FakeCluster()
    manager = make_manager(tmp_path, cluster)
    client = DirectManagerClient(manager)

    await client.put_json(
        f"/cluster/server/{ServerNum(7)}/start",
        {
            "expected_error": "boom",
            "seeds": [IPAddress("127.0.0.11")],
            "expected_server_up_state": ServerUpState.HOST_ID_QUERIED.name,
            "cmdline_options_override": ["--option"],
            "append_env_override": {"A": "B"},
            "auth_provider": {"authenticator": "builtins.dict", "kwargs": {}},
        },
    )

    assert cluster.server_start_calls == [
        {
            "server_id": ServerNum(7),
            "expected_error": "boom",
            "seeds": [IPAddress("127.0.0.11")],
            "expected_server_up_state": ServerUpState.HOST_ID_QUERIED,
            "cmdline_options_override": ["--option"],
            "append_env_override": {"A": "B"},
            "auth_provider": {"authenticator": "builtins.dict", "kwargs": {}},
        }
    ]

    await client.put_json(
        "/cluster/remove-node/1",
        {
            "server_id": ServerNum(2),
            "ignore_dead": [IPAddress("127.0.0.3")],
            "expected_error": None,
        },
    )

    assert cluster.api_calls == [
        (IPAddress("127.0.0.1"), HostID("host-2"), [IPAddress("127.0.0.3")], 1000)
    ]
    assert cluster.removed == {ServerNum(2)}


@pytest.mark.asyncio
async def test_direct_manager_client_getters(tmp_path: Path) -> None:
    cluster = FakeCluster()
    manager = make_manager(tmp_path, cluster)
    client = DirectManagerClient(manager)

    assert await client.get_json("/up") is True
    assert await client.get_json("/cluster/up") is True
    assert await client.get_json("/cluster/is-dirty") is False
    assert await client.get_json("/cluster/replicas") == 3
    assert await client.get_json("/cluster/host-ip/1") == IPAddress("127.0.0.1")
    assert await client.get_json("/cluster/host-id/2") == HostID("host-2")
