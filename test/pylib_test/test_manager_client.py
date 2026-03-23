#!/usr/bin/env python3
# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import pytest

from test.pylib.manager_client import ManagerClient


class FakeTransport:
    def __init__(self) -> None:
        self.get_calls = []
        self.shutdown_calls = 0

    async def get_json(
        self,
        resource_uri: str,
        host=None,
        port=None,
        params=None,
        allow_failed: bool = False,
    ):
        self.get_calls.append((resource_uri, host, port, params, allow_failed))
        if resource_uri == "/up":
            return True
        raise AssertionError(f"unexpected get_json call: {resource_uri}")

    async def put_json(
        self,
        resource_uri: str,
        data=None,
        host=None,
        port=None,
        params=None,
        response_type=None,
        timeout=None,
    ):
        raise AssertionError(f"unexpected put_json call: {resource_uri}")

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


@pytest.mark.asyncio
async def test_manager_client_uses_injected_transport_factory() -> None:
    created_clients = []

    def client_factory() -> FakeTransport:
        client = FakeTransport()
        created_clients.append(client)
        return client

    manager = ManagerClient(
        sock_path="/tmp/unused-manager.sock",
        port=9042,
        use_ssl=False,
        auth_provider=None,
        con_gen=lambda *_args, **_kwargs: None,
        client_factory=client_factory,
    )

    assert await manager.is_manager_up() is True
    assert len(created_clients) == 1
    assert created_clients[0].get_calls == [("/up", None, None, None, False)]

    await manager.stop()

    assert created_clients[0].shutdown_calls == 1
