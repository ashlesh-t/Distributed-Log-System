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
            container = client.containers.get(name)
            container.restart(timeout=10)
            print(f"[restart] Docker restarted container '{name}'")
        except Exception as exc:
            print(f"[restart] Docker restart failed for {node_id}: {exc}")

    async def _restart_k8s(self, service_name: str) -> None:
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
                name=service_name.lower(), namespace=namespace, body=patch
            )
            print(f"[restart] K8s rollout restart triggered for '{service_name}'")
        except Exception as exc:
            print(f"[restart] K8s restart failed for {service_name}: {exc}")
