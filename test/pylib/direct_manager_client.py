#!/usr/bin/env python3
# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import asyncio
from typing import Any, Mapping, Optional

from test.pylib.scylla_cluster import ScyllaClusterManager


class _Request:
    def __init__(
        self,
        path_qs: str,
        match_info: Optional[dict[str, str]] = None,
        data: Optional[Mapping[str, Any]] = None,
        query: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.path_qs = path_qs
        self.url = path_qs
        self.match_info = match_info or {}
        self._data = data or {}
        self.query = query or {}

    async def json(self) -> Mapping[str, Any]:
        return self._data

    def __repr__(self) -> str:
        return (
            f"_Request(path_qs={self.path_qs!r}, "
            f"match_info={self.match_info!r}, "
            f"query={self.query!r})"
        )

    def __str__(self) -> str:
        return repr(self)


class DirectManagerClient:
    """In-process adapter for ScyllaClusterManager request handlers."""

    def __init__(self, manager: ScyllaClusterManager, caller=None) -> None:
        self.manager = manager
        self._caller = caller

    async def _call(
        self,
        handler,
        path: str,
        *,
        blockable: bool = False,
        match_info: Optional[dict[str, str]] = None,
        data: Optional[Mapping[str, Any]] = None,
        query: Optional[Mapping[str, str]] = None,
    ) -> Any:
        request = _Request(path, match_info=match_info, data=data, query=query)
        task = asyncio.current_task()
        if blockable and self.manager.server_broken_event.is_set():
            raise Exception(
                f"ScyllaClusterManager BROKEN, Previous test broke ScyllaClusterManager server, server_broken_reason: {self.manager.server_broken_reason}"
            )
        self.manager.logger.info(
            "[ScyllaClusterManager][%s] %s",
            task.get_name() if task else "direct",
            request.url,
        )
        if task is not None:
            self.manager.tasks_history[task] = request
        return await handler(request)

    async def _dispatch(
        self,
        handler,
        path: str,
        *,
        blockable: bool = False,
        match_info: Optional[dict[str, str]] = None,
        data: Optional[Mapping[str, Any]] = None,
        query: Optional[Mapping[str, str]] = None,
    ) -> Any:
        if self._caller is not None:
            return self._caller(
                self._call(
                    handler,
                    path,
                    blockable=blockable,
                    match_info=match_info,
                    data=data,
                    query=query,
                )
            )
        return await self._call(
            handler,
            path,
            blockable=blockable,
            match_info=match_info,
            data=data,
            query=query,
        )

    async def get_json(
        self,
        resource_uri: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        params: Optional[Mapping[str, str]] = None,
        allow_failed: bool = False,
    ) -> Any:
        del host, port, allow_failed
        if resource_uri == "/up":
            return await self._dispatch(self.manager._manager_up, resource_uri)
        if resource_uri == "/cluster/up":
            return await self._dispatch(self.manager._cluster_up, resource_uri)
        if resource_uri == "/cluster/is-dirty":
            return await self._dispatch(self.manager._is_dirty, resource_uri)
        if resource_uri == "/cluster/replicas":
            return await self._dispatch(self.manager._cluster_replicas, resource_uri)
        if resource_uri == "/cluster/running-servers":
            return await self._dispatch(
                self.manager._cluster_running_servers, resource_uri
            )
        if resource_uri == "/cluster/all-servers":
            return await self._dispatch(self.manager._cluster_all_servers, resource_uri)
        if resource_uri == "/cluster/starting-servers":
            return await self._dispatch(
                self.manager._cluster_starting_servers, resource_uri
            )
        if resource_uri.startswith("/cluster/host-ip/"):
            server_id = resource_uri.rsplit("/", 1)[1]
            return await self._dispatch(
                self.manager._cluster_server_ip_addr,
                resource_uri,
                match_info={"server_id": server_id},
            )
        if resource_uri.startswith("/cluster/host-id/"):
            server_id = resource_uri.rsplit("/", 1)[1]
            return await self._dispatch(
                self.manager._cluster_host_id,
                resource_uri,
                match_info={"server_id": server_id},
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/get_config"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_get_config,
                resource_uri,
                match_info={"server_id": server_id},
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/get_log_filename"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_get_log_filename,
                resource_uri,
                match_info={"server_id": server_id},
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/workdir"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_get_workdir,
                resource_uri,
                match_info={"server_id": server_id},
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/maintenance_socket_path"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_get_maintenance_socket_path,
                resource_uri,
                match_info={"server_id": server_id},
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/exe"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_get_exe,
                resource_uri,
                match_info={"server_id": server_id},
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/sstables_disk_usage"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_get_sstables_disk_usage,
                resource_uri,
                match_info={"server_id": server_id},
                query=params,
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/process_status"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_get_process_status,
                resource_uri,
                match_info={"server_id": server_id},
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/returncode"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_get_returncode,
                resource_uri,
                match_info={"server_id": server_id},
            )
        raise NotImplementedError(resource_uri)

    async def put_json(
        self,
        resource_uri: str,
        data: Optional[Mapping[str, Any]] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        params: Optional[dict[str, str]] = None,
        response_type: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        del host, port, params, response_type, timeout
        if resource_uri.startswith("/cluster/before-test/"):
            test_case_name = resource_uri.split("/cluster/before-test/", 1)[1]
            return await self._dispatch(
                self.manager._before_test_req,
                resource_uri,
                blockable=True,
                match_info={"test_case_name": test_case_name},
                data=data,
            )
        if resource_uri.startswith("/cluster/after-test/"):
            success = resource_uri.split("/cluster/after-test/", 1)[1]
            return await self._dispatch(
                self.manager._after_test,
                resource_uri,
                blockable=True,
                match_info={"success": success},
                data=data,
            )
        if resource_uri == "/cluster/mark-dirty":
            return await self._dispatch(
                self.manager._mark_dirty, resource_uri, blockable=True, data=data
            )
        if resource_uri == "/cluster/mark-clean":
            return await self._dispatch(
                self.manager._mark_clean, resource_uri, blockable=True, data=data
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/stop_gracefully"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._cluster_server_stop_gracefully,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/stop"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._cluster_server_stop,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/start"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._cluster_server_start,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/pause"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._cluster_server_pause,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/unpause"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._cluster_server_unpause,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri == "/cluster/addserver":
            return await self._dispatch(
                self.manager._cluster_server_add,
                resource_uri,
                blockable=True,
                data=data,
            )
        if resource_uri == "/cluster/addservers":
            return await self._dispatch(
                self.manager._cluster_servers_add,
                resource_uri,
                blockable=True,
                data=data,
            )
        if resource_uri.startswith("/cluster/remove-node/"):
            initiator = resource_uri.rsplit("/", 1)[1]
            return await self._dispatch(
                self.manager._cluster_remove_node,
                resource_uri,
                blockable=True,
                match_info={"initiator": initiator},
                data=data,
            )
        if resource_uri.startswith("/cluster/decommission-node/"):
            server_id = resource_uri.rsplit("/", 1)[1]
            return await self._dispatch(
                self.manager._cluster_decommission_node,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri.startswith("/cluster/rebuild-node/"):
            server_id = resource_uri.rsplit("/", 1)[1]
            return await self._dispatch(
                self.manager._cluster_rebuild_node,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/update_config"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_update_config,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/remove_config_option"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_remove_config_option,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/update_cmdline"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_update_cmdline,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/switch_executable"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_switch_executable,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/change_ip"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_change_ip,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/change_rpc_address"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._server_change_rpc_address,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        if resource_uri.startswith("/cluster/server/") and resource_uri.endswith(
            "/wipe_sstables"
        ):
            server_id = resource_uri.split("/")[3]
            return await self._dispatch(
                self.manager._cluster_server_wipe_sstables,
                resource_uri,
                blockable=True,
                match_info={"server_id": server_id},
                data=data,
            )
        raise NotImplementedError(resource_uri)

    async def shutdown(self) -> None:
        pass
