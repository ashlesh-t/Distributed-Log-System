import os
import uuid
from pathlib import Path


def get_node_id(service_name: str) -> str:
    """Return a stable UUID for this service instance.

    Priority: NODE_ID env var → /tmp/<service>.uuid file → generate and persist.
    """
    if env_id := os.environ.get("NODE_ID"):
        return env_id

    path = Path(f"/tmp/{service_name}.uuid")
    if path.exists():
        return path.read_text().strip()

    node_id = str(uuid.uuid4())
    path.write_text(node_id)
    return node_id
