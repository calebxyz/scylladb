#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import ctypes
import dataclasses
import hashlib
from collections.abc import AsyncIterator, Sequence
from typing import Optional
from unittest import mock

import pytest
from cassandra import ProtocolVersion
from cassandra.application_info import ApplicationInfoBase
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.protocol import ResultMessage

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.pylib.manager_client import ManagerClient
from test.pylib.util import unique_name


class _UseMetadataId(ApplicationInfoBase):
    def add_startup_options(self, options: dict) -> None:
        options["SCYLLA_USE_METADATA_ID"] = ""


@pytest.fixture(scope="function", autouse=True)
async def prepare_3_nodes_cluster() -> None:
    # Override the cluster-level autouse fixture so this module can manage a
    # single cluster lifecycle across all parametrized cases.
    return None


@pytest.fixture(scope="function", autouse=True)
async def prepare_3_racks_cluster() -> None:
    # Override the cluster-level autouse fixture for the same reason as above.
    return None


_SIZE_T_BYTES = ctypes.sizeof(ctypes.c_size_t)
_UTF8_TYPE = "org.apache.cassandra.db.marshal.UTF8Type"
_BOOLEAN_TYPE = "org.apache.cassandra.db.marshal.BooleanType"
_DURATION_TYPE = "org.apache.cassandra.db.marshal.DurationType"
_INT32_TYPE = "org.apache.cassandra.db.marshal.Int32Type"
_TEXT_MAP_TYPE = f"org.apache.cassandra.db.marshal.MapType({_UTF8_TYPE},{_UTF8_TYPE})"

# Keep these schemas in sync with the corresponding get_result_metadata()
# implementations in cql3/statements/*.cc.
_LIST_ROLES_SCHEMA = (
    ("role", _UTF8_TYPE),
    ("super", _BOOLEAN_TYPE),
    ("login", _BOOLEAN_TYPE),
    ("options", _TEXT_MAP_TYPE),
)
# cql3/statements/list_users_statement.cc
_LIST_USERS_SCHEMA = (
    ("name", _UTF8_TYPE),
    ("super", _BOOLEAN_TYPE),
)
# cql3/statements/list_permissions_statement.cc
_LIST_PERMISSIONS_SCHEMA = (
    ("role", _UTF8_TYPE),
    ("username", _UTF8_TYPE),
    ("resource", _UTF8_TYPE),
    ("permission", _UTF8_TYPE),
)
# cql3/statements/list_service_level_statement.cc for LIST SERVICE LEVEL <name>
_LIST_SERVICE_LEVEL_SCHEMA = (
    ("service_level", _UTF8_TYPE),
    ("timeout", _DURATION_TYPE),
    ("workload_type", _UTF8_TYPE),
    ("shares", _INT32_TYPE),
)
# cql3/statements/list_service_level_statement.cc for LIST ALL SERVICE LEVELS
_LIST_ALL_SERVICE_LEVELS_SCHEMA = (
    *_LIST_SERVICE_LEVEL_SCHEMA,
    ("percentage of all service level shares", _UTF8_TYPE),
)
# cql3/statements/list_service_level_attachments_statement.cc
_LIST_ATTACHED_SERVICE_LEVEL_SCHEMA = (
    ("role", _UTF8_TYPE),
    ("service_level", _UTF8_TYPE),
)
# cql3/statements/list_effective_service_level_statement.cc
_LIST_EFFECTIVE_SERVICE_LEVEL_SCHEMA = (
    ("service_level_option", _UTF8_TYPE),
    ("effective_service_level", _UTF8_TYPE),
    ("value", _UTF8_TYPE),
)


@dataclasses.dataclass
class _ExecuteResult:
    prepared_metadata_id: Optional[bytes]
    result_metadata_id: Optional[bytes]
    metadata_changed: bool
    row_count: int


@dataclasses.dataclass(frozen=True)
class _PreparedListMetadataCase:
    query_template: str
    expected_metadata_id: bytes


@dataclasses.dataclass(frozen=True)
class _PreparedListContext:
    server_ip: str
    role: str
    service_level: str


def _feed_string_for_metadata_id(hasher, value: str) -> None:
    encoded = value.encode("utf-8")
    hasher.update(
        len(encoded).to_bytes(_SIZE_T_BYTES, byteorder="little", signed=False)
    )
    hasher.update(encoded)


def _calculate_metadata_id(columns: Sequence[tuple[str, str]]) -> bytes:
    # Match cql3::metadata::calculate_metadata_id() and appending_hash<std::string>.
    hasher = hashlib.sha256()
    for column_name, type_name in columns:
        _feed_string_for_metadata_id(hasher, column_name)
        _feed_string_for_metadata_id(hasher, type_name)
    return hasher.digest()[:16]


_LIST_METADATA_CASES = [
    pytest.param(
        _PreparedListMetadataCase(
            query_template="LIST ROLES OF {role}",
            expected_metadata_id=_calculate_metadata_id(_LIST_ROLES_SCHEMA),
        ),
        id="list_roles_of",
    ),
    pytest.param(
        _PreparedListMetadataCase(
            query_template="LIST USERS",
            expected_metadata_id=_calculate_metadata_id(_LIST_USERS_SCHEMA),
        ),
        id="list_users",
    ),
    pytest.param(
        _PreparedListMetadataCase(
            query_template="LIST ALL PERMISSIONS",
            expected_metadata_id=_calculate_metadata_id(_LIST_PERMISSIONS_SCHEMA),
        ),
        id="list_all_permissions",
    ),
    pytest.param(
        _PreparedListMetadataCase(
            query_template="LIST SERVICE LEVEL {service_level}",
            expected_metadata_id=_calculate_metadata_id(_LIST_SERVICE_LEVEL_SCHEMA),
        ),
        id="list_service_level",
    ),
    pytest.param(
        _PreparedListMetadataCase(
            query_template="LIST ALL SERVICE LEVELS",
            expected_metadata_id=_calculate_metadata_id(
                _LIST_ALL_SERVICE_LEVELS_SCHEMA
            ),
        ),
        id="list_all_service_levels",
    ),
    pytest.param(
        _PreparedListMetadataCase(
            query_template="LIST ATTACHED SERVICE LEVEL OF {role}",
            expected_metadata_id=_calculate_metadata_id(
                _LIST_ATTACHED_SERVICE_LEVEL_SCHEMA
            ),
        ),
        id="list_attached_service_level",
    ),
    pytest.param(
        _PreparedListMetadataCase(
            query_template="LIST EFFECTIVE SERVICE LEVEL OF {role}",
            expected_metadata_id=_calculate_metadata_id(
                _LIST_EFFECTIVE_SERVICE_LEVEL_SCHEMA
            ),
        ),
        id="list_effective_service_level",
    ),
]


def _prepare_and_execute(host: str, query: str) -> _ExecuteResult:
    captured = {"metadata_id": None, "metadata_changed": False}
    original_recv = ResultMessage.recv_results_metadata

    def _capturing_recv(self: ResultMessage, f, user_type_map) -> None:
        original_recv(self, f, user_type_map)
        metadata_id = getattr(self, "result_metadata_id", None)
        if metadata_id is not None:
            captured["metadata_id"] = metadata_id
            captured["metadata_changed"] = True

    with mock.patch.object(
        ProtocolVersion, "uses_prepared_metadata", staticmethod(lambda _: True)
    ):
        cluster = Cluster(
            contact_points=[host],
            port=9042,
            protocol_version=4,
            auth_provider=PlainTextAuthProvider("cassandra", "cassandra"),
            application_info=_UseMetadataId(),
            load_balancing_policy=WhiteListRoundRobinPolicy([host]),
        )
        session = cluster.connect()
        try:
            prepared = session.prepare(query)
            with mock.patch.object(
                ResultMessage, "recv_results_metadata", _capturing_recv
            ):
                rows = list(session.execute(prepared))
            return _ExecuteResult(
                prepared_metadata_id=prepared.result_metadata_id,
                result_metadata_id=captured["metadata_id"],
                metadata_changed=captured["metadata_changed"],
                row_count=len(rows),
            )
        finally:
            session.shutdown()
            cluster.shutdown()


@pytest.fixture(scope="module")
async def prepared_list_context(
    manager_internal,
    request: pytest.FixtureRequest,
) -> AsyncIterator[_PreparedListContext]:
    manager = manager_internal()
    module_test_name = "prepared_list_metadata_module"
    initial_test_failed = request.session.testsfailed
    role = "r" + unique_name()
    service_level = "sl" + unique_name()

    # Mirror ManagerClient.before_test() without installing an extra per-test
    # log handler: close any stale driver connection and lease a clean cluster.
    if await manager.is_dirty():
        manager.driver_close()
    await manager.client.put_json(
        f"/cluster/before-test/{module_test_name}", timeout=600, response_type="json"
    )
    servers = await manager.running_servers()
    if servers:
        await manager.driver_connect()
    cql = None
    try:
        servers = await manager.running_servers()
        if servers:
            server = servers[0]
        else:
            server = await manager.server_add(config=auth_config)

        cql, _ = await manager.get_ready_cql([server])
        assert cql
        await cql.run_async(
            f"CREATE ROLE {role} WITH PASSWORD = '{role}' AND LOGIN = true"
        )
        await cql.run_async(f"GRANT SELECT ON ALL KEYSPACES TO {role}")
        await cql.run_async(
            f"CREATE SERVICE LEVEL {service_level} WITH TIMEOUT = 10s AND WORKLOAD_TYPE = 'batch' AND SHARES = 100"
        )
        await cql.run_async(f"ATTACH SERVICE LEVEL {service_level} TO {role}")
        yield _PreparedListContext(
            server_ip=server.ip_addr, role=role, service_level=service_level
        )
        await cql.run_async(f"DETACH SERVICE LEVEL FROM {role}")
        await cql.run_async(f"DROP SERVICE LEVEL IF EXISTS {service_level}")
        await cql.run_async(f"DROP ROLE IF EXISTS {role}")
    finally:
        try:
            await manager.client.put_json(
                f"/cluster/after-test/{initial_test_failed == request.session.testsfailed}",
                response_type="json",
            )
            manager.test_finished_event.set()
        finally:
            await manager.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _LIST_METADATA_CASES)
async def test_prepared_list_metadata_ids(
    prepared_list_context: _PreparedListContext,
    case: _PreparedListMetadataCase,
) -> None:
    query = case.query_template.format(
        role=prepared_list_context.role,
        service_level=prepared_list_context.service_level,
    )
    # _prepare_and_execute() uses the synchronous Python driver, so run it in
    # a worker thread instead of blocking the asyncio-based test harness.
    result = await asyncio.to_thread(
        _prepare_and_execute, prepared_list_context.server_ip, query
    )
    assert result.row_count > 0, query
    assert result.prepared_metadata_id is not None, query
    assert result.prepared_metadata_id == case.expected_metadata_id, query
    assert not result.metadata_changed, query
    assert result.result_metadata_id is None, query
