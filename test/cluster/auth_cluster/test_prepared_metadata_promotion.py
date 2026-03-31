#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import dataclasses
from typing import Optional
from unittest import mock

import pytest
from cassandra import ProtocolVersion
from cassandra.application_info import ApplicationInfoBase
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.protocol import ResultMessage

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import unique_name
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config


# ---------------------------------------------------------------------------
# Driver helpers for SCYLLA_USE_METADATA_ID / result_metadata_id exchange.
#
# The standard Python driver gates result_metadata_id exchange on protocol v5+
# (ProtocolVersion.uses_prepared_metadata).  ScyllaDB does not implement v5
# but exposes the same semantics on v4 via the SCYLLA_USE_METADATA_ID startup
# extension.  Two lightweight patches make the driver exercise this path:
#
#   1. _UseMetadataId — ApplicationInfoBase subclass that injects
#      SCYLLA_USE_METADATA_ID into the STARTUP options dict.  Passed to
#      Cluster(application_info=...).  The driver merges these options into
#      the STARTUP frame without any filtering.
#
#   2. mock.patch.object(ProtocolVersion, "uses_prepared_metadata", ...) —
#      makes the driver write result_metadata_id in EXECUTE frames and read
#      it back from PREPARE/ROWS responses, which is exactly the v4 extension
#      wire format.
#
# Note: the driver does not send the SKIP_METADATA flag in EXECUTE even with
# these patches (it never ORs it into the options flags byte for prepared
# statements).  The server does not require SKIP_METADATA to trigger
# promotion; without it, it returns full column metadata alongside
# METADATA_CHANGED.
# ---------------------------------------------------------------------------


class _UseMetadataId(ApplicationInfoBase):
    """Inject SCYLLA_USE_METADATA_ID into the CQL STARTUP options."""

    def add_startup_options(self, options: dict) -> None:
        options["SCYLLA_USE_METADATA_ID"] = ""


@dataclasses.dataclass
class _ExecuteResult:
    """Parsed outcome of interest from a prepared-statement EXECUTE."""

    initial_metadata_id: Optional[bytes]
    """result_metadata_id returned by the PREPARE response."""

    result_metadata_id: Optional[bytes]
    """result_metadata_id embedded in the ROWS EXECUTE response, if any."""

    metadata_changed: bool
    """True when the ROWS response carried METADATA_ID_FLAG (promotion occurred)."""


def _prepare_and_execute(host: str, query: str) -> _ExecuteResult:
    """
    Connect via the Scylla Python driver with SCYLLA_USE_METADATA_ID negotiated,
    prepare *query*, execute it once, and return relevant metadata_id fields.

    Intended to be called via ``asyncio.to_thread`` to avoid blocking the event loop.

    The function uses two patches scoped to the connection lifetime:

    * ``ProtocolVersion.uses_prepared_metadata`` is forced to return ``True``
      for all protocol versions so that the driver reads/writes result_metadata_id
      in PREPARE and EXECUTE frames on protocol v4.

    * ``ResultMessage.recv_results_metadata`` is wrapped to capture
      result_metadata_id from the ROWS response (the driver parses it there but
      does not propagate it back to the PreparedStatement in the normal rows path).
    """
    captured: dict = {"metadata_id": None, "metadata_changed": False}
    original_recv = ResultMessage.recv_results_metadata

    def _capturing_recv(self: ResultMessage, f, user_type_map) -> None:
        original_recv(self, f, user_type_map)
        rmi = getattr(self, "result_metadata_id", None)
        if rmi is not None:
            captured["metadata_id"] = rmi
            captured["metadata_changed"] = True

    with mock.patch.object(
        ProtocolVersion,
        "uses_prepared_metadata",
        staticmethod(lambda v: True),
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
            ps = session.prepare(query)
            initial_metadata_id = ps.result_metadata_id
            with mock.patch.object(
                ResultMessage, "recv_results_metadata", _capturing_recv
            ):
                session.execute(ps)
            return _ExecuteResult(
                initial_metadata_id=initial_metadata_id,
                result_metadata_id=captured["metadata_id"],
                metadata_changed=captured["metadata_changed"],
            )
        finally:
            session.shutdown()
            cluster.shutdown()


@pytest.mark.asyncio
async def test_list_roles_of_prepared_metadata_promotion(
    manager: ManagerClient,
) -> None:
    """Verify that the server promotes the prepared metadata_id for statements
    whose PREPARE response carries empty result metadata.

    ``LIST ROLES OF <role>`` is such a statement: at PREPARE time the server
    does not know the result set schema because the statement implementation
    builds the metadata dynamically at execute time.  The server therefore
    returns the metadata_id of empty metadata in the PREPARE response.

    When the client later sends EXECUTE with the stale empty metadata_id, the
    server should detect the mismatch (the actual rows have real metadata) and
    respond with a ``METADATA_CHANGED`` result that carries the real
    metadata_id so the client can update its cache.  This is the behaviour
    mandated by CQL v5; on CQL v4 it is exercised via the
    SCYLLA_USE_METADATA_ID Scylla protocol extension which enables the same
    wire-level exchange.
    """
    server = await manager.server_add(config=auth_config)
    cql, _ = await manager.get_ready_cql([server])

    role = "r" + unique_name()
    await cql.run_async(f"CREATE ROLE {role}")

    result = await asyncio.to_thread(
        _prepare_and_execute, server.ip_addr, f"LIST ROLES OF {role}"
    )

    assert result.metadata_changed, (
        f"expected EXECUTE for 'LIST ROLES OF {role}' to return METADATA_CHANGED "
        f"after PREPARE returned an empty result_metadata_id"
    )
    assert result.result_metadata_id is not None, (
        f"expected EXECUTE for 'LIST ROLES OF {role}' to return a result_metadata_id "
        f"alongside METADATA_CHANGED"
    )
    assert result.initial_metadata_id != result.result_metadata_id, (
        f"expected promoted result_metadata_id to differ from the stale empty one "
        f"returned by PREPARE"
    )


@pytest.mark.asyncio
@pytest.mark.skip_mode(
    mode="release", reason="error injection is disabled in release mode"
)
async def test_list_roles_of_prepared_metadata_promotion_suppressed_by_injection(
    manager: ManagerClient,
) -> None:
    """Verify that the ``skip_prepared_result_metadata_promotion`` error injection
    suppresses the metadata promotion, leaving the response without METADATA_CHANGED.

    This is the negative/regression counterpart of
    ``test_list_roles_of_prepared_metadata_promotion``: it confirms that the
    happy-path test is not a false positive by showing that the promotion can
    be disabled, and that the injection point itself works correctly.

    Unlike injecting ``skip_rows_metadata_changed_response`` (which only
    suppresses the wire encoding), this injection skips the entire promotion
    block — no real_id is computed, the cache is not updated, and the response
    falls through to the normal path with no METADATA_CHANGED side effects.
    """
    server = await manager.server_add(config=auth_config)
    cql, _ = await manager.get_ready_cql([server])

    role = "r" + unique_name()
    await cql.run_async(f"CREATE ROLE {role}")

    async with inject_error(
        manager.api, server.ip_addr, "skip_prepared_result_metadata_promotion"
    ):
        result = await asyncio.to_thread(
            _prepare_and_execute,
            server.ip_addr,
            f"LIST ROLES OF {role}",
        )

    assert not result.metadata_changed, (
        f"expected injected EXECUTE for 'LIST ROLES OF {role}' to suppress "
        f"METADATA_CHANGED, but the flag was set"
    )
