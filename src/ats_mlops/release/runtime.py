"""Host Docker/Registry adapters. Serving receives neither socket nor credentials."""

import json
from pathlib import Path
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from .state import ReleaseError, atomic_json, canonical, require, sha


def http_json(url, payload=None, method=None):
    request = Request(url, data=None if payload is None else json.dumps(payload).encode(),
                      headers={"Content-Type": "application/json"}, method=method)
    with urlopen(request, timeout=5) as response:
        return json.load(response)


class Registry:
    def __init__(self, config):
        self.config = config

    def get(self):
        query = urlencode({"name": self.config["model_name"], "alias": self.config["alias"]})
        try:
            value = http_json(self.config["registry_uri"] + "/api/2.0/mlflow/registered-models/alias?" + query)
            return str(value["model_version"]["version"])
        except HTTPError as exc:
            try:
                error = json.loads(exc.read())
                # MLflow 3.10 returns HTTP 400 for a missing alias, rather than 404.
                missing = (error.get("error_code") == "INVALID_PARAMETER_VALUE"
                           and error.get("message") == f"Registered model alias {self.config['alias']} not found.")
                if exc.code == 400 and missing:
                    return None
            except (ValueError, TypeError):
                pass
            finally:
                exc.close()
            raise ReleaseError("registry_unavailable") from exc
        except Exception as exc:
            raise ReleaseError("registry_unavailable") from exc

    def set(self, version):
        body = {"name": self.config["model_name"], "alias": self.config["alias"]}
        if version is not None:
            body["version"] = str(version)
        try:
            http_json(self.config["registry_uri"] + "/api/2.0/mlflow/registered-models/alias", body,
                      "DELETE" if version is None else "POST")
            require(self.get() == version, "registry_confirmation_failed")
        except ReleaseError:
            raise
        except Exception as exc:
            raise ReleaseError("registry_unavailable") from exc


def docker(*args):
    result = subprocess.run(["docker", *map(str, args)], capture_output=True, text=True, timeout=90)
    require(result.returncode == 0, "docker_operation_failed")
    return result.stdout.strip()


class DockerRuntime:
    def __init__(self, root, config):
        self.root, self.config = Path(root), config

    def ids(self):
        return docker("ps", "-aq", "--filter", "label=com.docker.compose.project=" + self.config["project"],
                      "--filter", "label=com.docker.compose.service=ranking").split()

    def stop(self):
        for container in self.ids():
            owner = docker("inspect", "--format", '{{index .Config.Labels "ats.release.owner"}}', container)
            require(owner == str(self.root), "foreign_container")
            docker("rm", "-f", container)
        # Keep the named network and event volume across every switch and rollback.

    def launch(self, reference):
        launch = json.loads((self.root / reference["launch_path"]).read_text())
        require(launch["config_sha256"] == sha(canonical(self.config)), "config_changed")
        image = launch["image_id"]
        # Initialize ownership on the Linux named volume; no host SQLite/WAL bind mount.
        volumes = docker("volume", "ls", "-q", "--filter", "name=^" + self.config["events_volume"] + "$").split()
        if volumes:
            require(docker("volume", "inspect", "--format", '{{index .Labels "ats.release.owner"}}',
                           self.config["events_volume"]) == str(self.root), "foreign_event_volume")
        else:
            docker("volume", "create", "--label", "ats.release.owner=" + str(self.root), self.config["events_volume"])
        networks = docker("network", "ls", "-q", "--filter", "name=^" + self.config["network"] + "$").split()
        if networks:
            require(docker("network", "inspect", "--format", '{{index .Labels "ats.release.owner"}}',
                           self.config["network"]) == str(self.root), "foreign_serving_network")
        else:
            # Docker Desktop needs a normal bridge for published loopback ports.
            docker("network", "create", "--driver", "bridge", "--label", "ats.release.owner=" + str(self.root), self.config["network"])
        docker("run", "--rm", "--network", "none", "--user", "0:0", "--entrypoint", "/bin/sh",
               "--mount", "type=volume,src=" + self.config["events_volume"] + ",dst=/events",
               image, "-c", "chown 50000:50000 /events && chmod 700 /events")
        spec = self.compose(launch)
        directory = self.root / "runtime"
        directory.mkdir(exist_ok=True)
        atomic_json(directory / "compose.json", spec)
        docker("compose", "--env-file", "/dev/null", "-p", self.config["project"], "-f", directory / "compose.json",
               "up", "-d", "--no-deps", "--pull", "never", "ranking")

    def compose(self, launch):
        package = str(self.root / launch["package_path"])
        return {"services": {"ranking": {
            "image": launch["image_id"], "read_only": True, "user": "50000:50000",
            "labels": {"ats.release.owner": str(self.root)},
            "ports": [{"target": 3000, "published": str(self.config["port"]), "host_ip": "127.0.0.1"}],
            "tmpfs": ["/tmp:uid=50000,gid=50000", "/runtime:uid=50000,gid=50000"],
            "volumes": [{"type": "bind", "source": package, "target": "/release", "read_only": True},
                        {"type": "bind", "source": package + "/bentoml/models", "target": "/models", "read_only": True},
                        {"type": "volume", "source": "events", "target": "/events"}],
            "cap_drop": ["ALL"], "security_opt": ["no-new-privileges:true"],
            "cpus": 1, "mem_limit": "1536m", "restart": "unless-stopped"}},
            "volumes": {"events": {"external": True, "name": self.config["events_volume"]}},
            "networks": {"default": {"name": self.config["network"], "external": True}}}

    def observe(self):
        containers = self.ids()
        require(len(containers) <= 1, "multiple_serving_containers")
        if not containers:
            try:
                http_json(f"http://127.0.0.1:{self.config['port']}/metadata", {})
            except (OSError, URLError, HTTPError):
                return None
            raise ReleaseError("foreign_service_on_port")
        container = containers[0]
        raw = docker("inspect", "--format", '{{json .State.Running}}|{{.Image}}|{{json .Mounts}}|{{json .HostConfig.PortBindings}}|{{index .Config.Labels "ats.release.owner"}}', container)
        running, image, mounts, ports, owner = raw.split("|", 4)
        require(owner == str(self.root), "foreign_container")
        if running != "true":
            return {"ready": False}
        try:
            metadata = http_json(f"http://127.0.0.1:{self.config['port']}/metadata", {})
        except (OSError, URLError, HTTPError):
            return {"ready": False}
        return {"metadata": metadata, "image_id": image, "mounts": json.loads(mounts),
                "ports": json.loads(ports), "ready": metadata.get("ready") is True}

    def matches(self, observed, reference):
        if not observed or not observed.get("ready"):
            return False
        metadata = observed["metadata"]
        if any(metadata.get(key) != value for key, value in reference["metadata"].items()):
            return False
        if observed["image_id"] != reference["image_id"]:
            return False
        mounts = {item["Destination"]: item for item in observed["mounts"]}
        package = str(self.root / reference["package_path"])
        return (all(mounts.get(target, {}).get("Source") == source and mounts[target]["RW"] is False
                    for target, source in (("/release", package), ("/models", package + "/bentoml/models")))
                and mounts.get("/events", {}).get("Name") == self.config["events_volume"]
                and observed["ports"].get("3000/tcp") == [{"HostIp": "127.0.0.1", "HostPort": str(self.config["port"])}])

    def wait(self, reference):
        deadline = time.monotonic() + self.config["ready_timeout_seconds"]
        while time.monotonic() < deadline:
            observed = self.observe()
            if self.matches(observed, reference):
                return observed
            time.sleep(0.25)
        raise ReleaseError("new_service_not_ready")
