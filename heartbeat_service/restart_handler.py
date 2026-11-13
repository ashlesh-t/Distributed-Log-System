import asyncio
import os


class RestartHandler:
    def __init__(self):
        self._backend = os.environ.get("RESTART_BACKEND", "docker")

    async def restart(self, node_id: str, service_name: str) -> bool:
        """Restart the workload behind a node. Returns True only on success."""
        if self._backend == "kubernetes":
            return await self._restart_k8s(service_name)
        return await self._restart_docker(node_id, service_name)

    async def _restart_docker(self, node_id: str, service_name: str) -> bool:
        loop = asyncio.get_running_loop()
        try:
            name = await loop.run_in_executor(
                None, self._docker_restart_blocking, node_id, service_name
            )
            print(f"[restart] Docker restarted container '{name}'")
            return True
        except Exception as exc:
            print(f"[restart] Docker restart failed for {node_id}: {exc}")
            return False

    @staticmethod
    def _docker_restart_blocking(node_id: str, service_name: str) -> str:
        import docker  # type: ignore

        client = docker.from_env()
        override = os.environ.get(f"CONTAINER_{node_id.replace('-', '_')}")
        if override:
            container = client.containers.get(override)
        else:
            # Compose names containers <project>-<service>-<n>; match on label.
            matches = client.containers.list(
                all=True,
                filters={"label": f"com.docker.compose.service={service_name.lower()}"},
            )
            if not matches:
                # Fallback for non-compose deployments using plain container names.
                container = client.containers.get(service_name.lower())
            else:
                container = matches[0]
        container.restart(timeout=10)
        return container.name

    async def _restart_k8s(self, service_name: str) -> bool:
        try:
            from kubernetes import client as k8s_client, config as k8s_config  # type: ignore
            from datetime import datetime, timezone

            try:
                k8s_config.load_incluster_config()
            except Exception:
                k8s_config.load_kube_config()

            namespace = os.environ.get("K8S_NAMESPACE", "distributed-logs")
            apps = k8s_client.AppsV1Api()
            patch = {
                "spec": {
                    "template": {
                        "metadata": {
                            "annotations": {
                                "kubectl.kubernetes.io/restartedAt": datetime.now(
                                    timezone.utc
                                ).isoformat()
                            }
                        }
                    }
                }
            }
            apps.patch_namespaced_deployment(
                name=service_name.lower().replace("_", "-"),
                namespace=namespace,
                body=patch,
            )
            print(f"[restart] K8s rollout restart triggered for '{service_name}'")
            return True
        except Exception as exc:
            print(f"[restart] K8s restart failed for {service_name}: {exc}")
            return False
