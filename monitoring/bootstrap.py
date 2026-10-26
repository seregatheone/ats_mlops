"""Manage only the ATS monitoring project; never print rendered config or secrets."""

import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess


ROOT = Path(__file__).resolve().parents[1]
PYTHON = "python@sha256:e88e9763f943ec1834f992a4b51e0f24500486803e8bc534e5767af9ea65f6ce"


def run(arguments, **kwargs):
    result = subprocess.run(arguments, capture_output=True, **kwargs)
    if result.returncode:
        raise RuntimeError("monitoring_command_failed")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("up", "restart", "stop", "status", "config"))
    parser.add_argument("--state-dir", type=Path, default=Path.home() / ".local/state/ats-mlops-monitoring")
    parser.add_argument("--release-state", type=Path, default=Path.home() / ".local/state/ats-mlops")
    args = parser.parse_args(argv)
    state = args.state_dir.expanduser().resolve()
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    (state / "targets").mkdir(exist_ok=True, mode=0o755)
    if not (state / "targets/acceptance.json").exists():
        (state / "targets/acceptance.json").write_text("[]\n")
    images = json.loads((ROOT / "monitoring/images.json").read_text())
    env = dict(os.environ, ATS_MONITORING_SOURCE=str(ROOT), ATS_RELEASE_STATE=str(args.release_state.resolve()),
               ATS_MONITORING_STATE=str(state),
               ATS_PROMETHEUS_IMAGE=images["prometheus"]["reference"], ATS_GRAFANA_IMAGE=images["grafana"]["reference"])
    compose = ["docker", "compose", "--env-file", "/dev/null", "-p", "ats-monitoring", "-f", str(ROOT / "monitoring/compose.yaml")]
    if args.command == "up":
        password = state / "grafana-password"
        if not password.exists():
            with password.open("x") as stream:
                stream.write(secrets.token_urlsafe(32))
            password.chmod(0o600)
        # Only Grafana receives this separate native auth volume. Its password is
        # neither an environment value nor a host bind mount in any inference image.
        existing = run(["docker", "volume", "ls", "-q", "--filter", "name=^ats-monitoring-auth$"]).stdout.strip()
        if existing:
            owner = run(["docker", "volume", "inspect", "--format", '{{index .Labels "ats.monitoring.owner"}}', "ats-monitoring-auth"]).stdout.decode().strip()
            if owner != str(state):
                raise RuntimeError("foreign_monitoring_auth")
        else:
            run(["docker", "volume", "create", "--label", "ats.monitoring.owner=" + str(state), "ats-monitoring-auth"])
        code = ("import os,pathlib,sys;p=pathlib.Path('/auth/admin-password');b=sys.stdin.buffer.read();"
                "p.write_bytes(b);p.chmod(0o400);os.chown(p,472,0)")
        run(["docker", "run", "--rm", "-i", "--network", "none", "--memory", "64m", "--cpus", ".25",
             "--user", "0:0", "--entrypoint", "python", "--mount", "type=volume,src=ats-monitoring-auth,dst=/auth",
             PYTHON, "-I", "-B", "-c", code], input=password.read_bytes())
        command = ["up", "-d", "--pull", "never"]
    else:
        command = {"restart": ["restart"], "stop": ["stop"], "status": ["ps", "--format", "json"], "config": ["config", "--quiet"]}[args.command]
    result = run(compose + command, env=env)
    if args.command == "status":
        print(result.stdout.decode())
    else:
        print(json.dumps({"outcome": "passed", "command": args.command}))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print('{"outcome":"failed","reason":"monitoring_command_failed"}')
        raise SystemExit(1)
