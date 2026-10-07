import os


class RestartHandler:
    def __init__(self):
        self._backend = os.environ.get("RESTART_BACKEND", "docker")

    async def restart(self, node_id: str, service_name: str) -> None:
        if self._backend == "kubernetes":
            await self._restart_k8s(service_name)
        else:
            await self._restart_docker(node_id, service_name)

    async def _restart_docker(self, node_id: str, service_name: str) -> None:
        try:
            import docker  # type: ignore

            client = docker.from_env()
            name = os.environ.get(
                f"CONTAINER_{node_id.replace('-', '_')}", service_name.lower()
            )
            # Try the configured/bare name first (works when container_name is set
            # in docker-compose.yml).  Fall back to a partial-name scan to handle
            # Docker Compose project-prefixed names like "project-service-1".
            try:
                container = client.containers.get(name)
            except Exception:
                # Use Docker's native filtering for performance instead of fetching all
                # containers and doing string matching in Python.
                matches = client.containers.list(filters={"name": name})
                if not matches:
                    raise RuntimeError(f"No container found matching '{name}'")
                container = matches[0]
            container.restart(timeout=10)
            print(f"[restart] Docker restarted container '{name}'")
        except Exception as exc:
            print(f"[restart] Docker restart failed for {node_id}: {exc}")

    async def _restart_k8s(self, service_name: str) -> None:
        try:
            from datetime import datetime, timezone

            from kubernetes import client as k8s_client  # type: ignore
            from kubernetes import config as k8s_config

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
                name=service_name.lower(), namespace=namespace, body=patch
            )
            print(f"[restart] K8s rollout restart triggered for '{service_name}'")
        except Exception as exc:
            print(f"[restart] K8s restart failed for {service_name}: {exc}")
