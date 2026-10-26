# Локальная демонстрация жизненного цикла

Это инструкция подготовки, а не новый отчёт о выполненном clean-clone прогоне.
Она использует существующие CLI. [T18](acceptance.md) уже принят; показ ниже
не заменяет формальные [исследования T19](results.md) и не читает original test.
Данные для `/rank` синтетические, score не является вероятностью трудоустройства.

## Среда и отдельное состояние

Нужны Git, Docker/Compose и host Python для stdlib coordinator. ML-зависимости
строятся из lock в CPython3.11 image. Выполнять в Bash из clean committed clone
на собственном отдельном Docker runtime с достаточной памятью. Команды готовят
два заранее назначенных demo fits A/B, zero HPO; лучших по метрике не выбирают.
Все ненулевые неожиданные exits останавливают подготовку, исходы сохраняются.

На общем host задать и `--project`, и отдельный `--image-prefix`: prefix
сохраняется вместе с private core config и изолирует build tags. Не пересобирать
общие defaults и не останавливать чужие сервисы ради показа. Короткий путь ниже
использует свой core и release namespace; живой запуск Airflow с тем же явным
project описан отдельно в [runbook](runbook.md), его genuine T18 traces доступны
в [public audit](../reports/acceptance/t18/raw/acceptance-audit.json).

```sh
set -euo pipefail
T20_REPO="$(pwd -P)"
T20_SHA="$(git rev-parse HEAD)"
test -z "$(git status --porcelain --untracked-files=all)"
T20_ID="$(date -u +%Y%m%d%H%M%S)"
T20_ROOT="${XDG_STATE_HOME:-$HOME/.local/state}/ats-mlops-demo/$T20_ID"
T20_PROJECT="ats-demo-$T20_ID"
T20_CORE="${XDG_STATE_HOME:-$HOME/.local/state}/ats-mlops-demo-core/$T20_ID"
T20_ML_TAG="${T20_PROJECT}-ml:t4"
mkdir -p "$(dirname "$T20_ROOT")"
mkdir -m 700 "$T20_ROOT"
mkdir "$T20_ROOT/inputs" "$T20_ROOT/runs" "$T20_ROOT/releases" "$T20_ROOT/bundles"
mkdir "$T20_ROOT/bundles/staging"
export PYTHONPATH="$T20_REPO/src:$T20_REPO"
python3 -B scripts/bootstrap_core.py init --state-dir "$T20_CORE" --project "$T20_PROJECT" --image-prefix "$T20_PROJECT" --minio-port 19100 --console-port 19101 --mlflow-port 15150
python3 -B scripts/bootstrap_core.py config --state-dir "$T20_CORE" --project "$T20_PROJECT"
python3 -B scripts/bootstrap_core.py up --state-dir "$T20_CORE" --project "$T20_PROJECT"
python3 -B scripts/bootstrap_core.py status --state-dir "$T20_CORE" --project "$T20_PROJECT"
```

Порты должны быть свободны; при необходимости выбрать другие до первого init
и согласованно заменить их в release config ниже. Credentials bootstrap создаёт
в0600 и повторно использует. Не печатать полный env/inspect/Compose config.
`config` выполняет quiet validation, `down` сохраняет volumes.

Получить actual ML image identity, а не исторический ID из старого отчёта:

```sh
python3 - "$T20_ROOT/inputs/image.json" "$T20_ML_TAG" <<'PY'
import json,subprocess,sys
fmt='{"id":{{json .Id}},"repo_digests":{{json .RepoDigests}},"os":{{json .Os}},"architecture":{{json .Architecture}}}'
value=json.loads(subprocess.check_output(['docker','image','inspect','--format',fmt,sys.argv[2]]));value['reference']=sys.argv[2]
value['repo_digests']=value['repo_digests'] or []
with open(sys.argv[1],'x') as f: json.dump(value,f,sort_keys=True);f.write('\n')
PY
T20_ML_IMAGE="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["id"])' "$T20_ROOT/inputs/image.json")"
```

Для последующих ML команд source перекрывается readonly mount, workspace
остаётся writable. Host UID нужен для файлов своего private каталога; source
и writable output подставляются отдельно. Core credentials лежат вне общего
дерева demo и не монтируются: `--env-file` передаёт только DVC client-role.
Release journal/approvals также отсутствуют в ML mounts. Кеши остаются в tmpfs.

```sh
t20_ml() {
  local t20_source="$1" t20_write="$2"; shift 2
  docker run --rm --pull=never --network "${T20_PROJECT}_default" \
    --read-only --cpus 1 --memory 1536m --user "$(id -u):$(id -g)" \
    --tmpfs /tmp:rw,size=128m,mode=1777 --env-file "$T20_CORE/dvc.env" \
    -e MLFLOW_TRACKING_URI=http://mlflow:5000 -e ATS_ML_PYTHON=/opt/ml-venv/bin/python \
    -e PYTHONDONTWRITEBYTECODE=1 -e "PYTHONPATH=$t20_source/src:$t20_source" \
    -e XDG_CACHE_HOME=/tmp/cache -e BENTOML_HOME=/tmp/bento -e MPLCONFIGDIR=/tmp/mpl \
    -e OMP_NUM_THREADS=1 -e OPENBLAS_NUM_THREADS=1 -e MKL_NUM_THREADS=1 \
    -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e "GIT_CONFIG_VALUE_0=$t20_source" \
    --mount "type=bind,source=$T20_ROOT/inputs,target=$T20_ROOT/inputs,readonly" \
    --mount "type=bind,source=$T20_ROOT/bundles,target=$T20_ROOT/bundles,readonly" \
    --mount "type=bind,source=$T20_ROOT/runs,target=$T20_ROOT/runs,readonly" \
    --mount "type=bind,source=$T20_ROOT/releases,target=$T20_ROOT/releases,readonly" \
    --mount "type=bind,source=$t20_write,target=$t20_write" \
    --mount "type=bind,source=$t20_source,target=$t20_source,readonly" \
    --workdir "$t20_source" --entrypoint /opt/ml-venv/bin/python "$T20_ML_IMAGE" -B "$@"
}
cp tests/fixtures/data/ci-config.json "$T20_ROOT/inputs/ci-config.json"
t20_ml "$T20_REPO" "$T20_ROOT/bundles/staging" -m ats_mlops pipeline bundle --source-root "$T20_REPO" --config "$T20_ROOT/inputs/ci-config.json" --output "$T20_ROOT/bundles/staging/bundle"
T20_BUNDLE_SHA="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["sha256"])' "$T20_ROOT/bundles/staging/bundle/bundle.json")"
```

Это одноразовый admission synthetic fixture. Bundle передаёт train/validation
и metadata, без source/corpus/test raw. Новый пустой MinIO не требует авторских
objects: данные создаются локально до `pipeline init`. Если показывается DVC
remote roundtrip, настроить свой remote/endpoint из `core/client.json` и делать
push/pull только в mutable train/validation workspace, не в root checkout с
protected pointers. [Контракт DAG и кеша](reproducibility.md) остаётся тем же.

## Два фиксированных demo fits

```sh
for t20_slot in A B; do
  python3 -B -m ats_mlops pipeline init --repo "$T20_REPO" --git-sha "$T20_SHA" \
    --directory "$T20_ROOT/runs/$t20_slot" --bundle "$T20_ROOT/bundles/staging/bundle" \
    --bundle-sha "$T20_BUNDLE_SHA" --image-identity "$T20_ROOT/inputs/image.json"
  python3 - "$T20_ROOT/runs/$t20_slot/workspace/params.yaml" <<'PY'
import json,sys
params={'model':{'seed':20261026,'features':{'max_features':2000},'classifier':{'C':1.0}},
        'search':{'mode':'frozen'},'smoke':False,'evaluation':{'score_decimals':12}}
with open(sys.argv[1],'w') as f: json.dump(params,f,sort_keys=True);f.write('\n')
PY
  t20_ml "$T20_ROOT/runs/$t20_slot/source" "$T20_ROOT/runs/$t20_slot" -m ats_mlops pipeline repro \
    --workspace "$T20_ROOT/runs/$t20_slot/workspace" --source-root "$T20_ROOT/runs/$t20_slot/source" --force
done
```

Два distinct runs/versions берутся из `artifacts/training/final/run_manifest.json`.
Они нужны и для реального T12 alias-move check: `alias_other_version` должна
существовать и отличаться от candidate. Это отдельная подготовка demo, не
qualification5, formal E1 или измерение E2. Source checkout не редактируется.

## Generic image и request JSON

Сначала сверить сохранённый baseID с tag, затем построить generic serving image:

```sh
test "$(docker image inspect --format '{{.Id}}' "$T20_ML_TAG")" = "$T20_ML_IMAGE"
docker build --pull=false --network=none -f docker/serving/Dockerfile --build-arg BASE_IMAGE="$T20_ML_TAG" --build-arg SOURCE_SHA="$T20_SHA" -t "ats-serving:demo-$T20_ID" .
python3 - "$T20_ROOT" "ats-serving:demo-$T20_ID" "$T20_SHA" <<'PY'
import json,subprocess,sys
from pathlib import Path
root,tag,source=Path(sys.argv[1]),sys.argv[2],sys.argv[3]
fmt='{"id":{{json .Id}},"os":{{json .Os}},"architecture":{{json .Architecture}},"revision":{{json (index .Config.Labels "org.opencontainers.image.revision")}}}'
image=json.loads(subprocess.check_output(['docker','image','inspect','--format',fmt,tag]));assert image.pop('revision')==source
code="import json,sys;from pathlib import Path;sys.path.insert(0,'/opt/serving');from service.runtime import environment;from service.integrity import inventory;print(json.dumps({'source_sha256':inventory(Path('/opt/serving/service')),'environment':environment()}))"
runtime=json.loads(subprocess.check_output(['docker','run','--rm','--pull=never','--network','none','--read-only','--tmpfs','/tmp:rw,size=64m,mode=1777','--entrypoint','/opt/ml-venv/bin/python',image['id'],'-I','-B','-c',code]))
identity={'image':image,'git_sha':source,**runtime}
with (root/'inputs/serving.json').open('x') as f:json.dump(identity,f,sort_keys=True);f.write('\n')
PY
```

Следующая shell-функция только собирает JSON из фактических manifest и вызывает
публичные gate/package/check. Для B/C/D сначала проверяется реальный ACTIVE
incumbent через `release status`. Значения Registry version не захардкожены.

```sh
t20_release() { python3 -B -m ats_mlops release --state-dir "$T20_ROOT/release-state" "$@"; }
t20_prepare() {
  local t20_id="$1" t20_candidate="$2" t20_incumbent="$3" t20_inc_release="$4"
  if test "$t20_incumbent" != none; then t20_release status > "$T20_ROOT/inputs/current-status.json"; fi
  python3 - "$T20_ROOT" "$T20_REPO" "$T20_PROJECT" "$t20_id" "$t20_candidate" "$t20_incumbent" "$t20_inc_release" <<'PY'
import json,sys
from pathlib import Path
root,repo,project,release,candidate,inc,inc_release=Path(sys.argv[1]),Path(sys.argv[2]),*sys.argv[3:]
read=lambda p:json.loads(p.read_bytes())
def paths(slot):
 w=root/'runs'/slot/'workspace'
 return {'run_manifest':str(w/'artifacts/training/final/run_manifest.json'),'pipeline_manifest':str(w/'artifacts/evaluation/pipeline_manifest.json'),'model_dir':str(w/'artifacts/training/final/model')}
cand=paths(candidate);w=root/'runs'/candidate/'workspace';run=read(Path(cand['run_manifest']))
incumbent=None
if inc!='none':
 incumbent={**paths(inc),'release_id':inc_release,'release_is_fixture':False}
 active=read(root/'inputs/current-status.json')['active'];assert active['release_id']==inc_release
 assert active['candidate']['model_version']==read(Path(incumbent['run_manifest']))['mlflow']['model_version']
other=read(Path(paths('B' if candidate=='A' else 'A')['run_manifest']))['mlflow']['model_version']
assert other!=run['mlflow']['model_version']
out=root/'releases'/release;out.mkdir();(out/'payload').mkdir()
common={'validation_manifest':str(w/'artifacts/prepared/data_manifest.json'),'admission_report':str(w/'artifacts/prepared/admission.json'),'dataset_binding':str(w/'dataset-binding.json')}
requests={'gate-request.json':{**common,'thresholds':str(repo/'configs/quality.yaml'),'candidate':cand,'incumbent':incumbent},
 'package-request.json':{'release_id':release,'gate_report':str(out/'gate.json'),'run_manifest':cand['run_manifest'],'pipeline_manifest':cand['pipeline_manifest'],'serving_identity':str(root/'inputs/serving.json')},
 'check-request.json':{**common,'package':str(out/'payload/package'),'alias_other_version':other,'registry_transport':{'network':project+'_default','tracking_uri':'http://mlflow:5000'}}}
for name,value in requests.items():
 with (out/name).open('x') as f:json.dump(value,f,sort_keys=True);f.write('\n')
PY
  python3 -B -m ats_mlops.quality.gate --request "$T20_ROOT/releases/$t20_id/gate-request.json" --output "$T20_ROOT/releases/$t20_id/gate.json"
  t20_ml "$T20_REPO" "$T20_ROOT/releases/$t20_id/payload" -m service.package --request "$T20_ROOT/releases/$t20_id/package-request.json" --output "$T20_ROOT/releases/$t20_id/payload/package"
  python3 -B -m service.release_check --request "$T20_ROOT/releases/$t20_id/check-request.json" --output "$T20_ROOT/releases/$t20_id/check.json"
}
```

## Выпуск, запросы и откат

Создать собственный immutable release config, не использовать default champion:

```sh
python3 - "$T20_ROOT/inputs/release-config.json" "$T20_ID" <<'PY'
import json,sys
name='ats-demo-serving-'+sys.argv[2]
value={'schema_version':1,'project':name,'port':13150,'registry_uri':'http://127.0.0.1:15150','model_name':'ats-pair-ranking','alias':'demo_'+sys.argv[2],'events_volume':name+'-events','network':name+'-network','ready_timeout_seconds':60}
with open(sys.argv[1],'x') as f:json.dump(value,f,sort_keys=True);f.write('\n')
PY
T20_A="demo-a-$T20_ID"; T20_B="demo-b-$T20_ID"
T20_C="demo-c-$T20_ID"; T20_D="demo-d-$T20_ID"
t20_prepare "$T20_A" A none none
t20_release --config "$T20_ROOT/inputs/release-config.json" stage --package "$T20_ROOT/releases/$T20_A/payload/package" --release-check "$T20_ROOT/releases/$T20_A/check.json"
t20_release approve "$T20_A" --actor agent-operator
t20_release activate "$T20_A"
t20_release status
curl --fail --silent --show-error -H 'Content-Type: application/json' -d '{}' http://127.0.0.1:13150/metadata
```

Пример нового синтетического запроса:

```sh
curl --fail --silent --show-error -H 'Content-Type: application/json' \
  -d '{"request_id":"demo-rank-1","vacancy":{"id":"demo-v","text":"Python SQL"},"candidates":[{"id":"demo-c1","text":"Python SQL опыт"},{"id":"demo-c2","text":"Дизайн интерфейсов"}],"top_k":2}' \
  http://127.0.0.1:13150/rank
```

Повтор того же ID/входа возвращает исходный ответ. Другой payload с этим ID
должен дать422; для проверки текущей версии нужен новый ID, а не cached replay.
Для B и дополнительного recovery C использовать реальный ACTIVE A:

```sh
t20_prepare "$T20_B" B A "$T20_A"
t20_prepare "$T20_C" B A "$T20_A"
t20_release stage --package "$T20_ROOT/releases/$T20_B/payload/package" --release-check "$T20_ROOT/releases/$T20_B/check.json"
t20_release approve "$T20_B" --actor agent-operator
t20_release activate "$T20_B"
t20_release status
t20_prepare "$T20_D" A B "$T20_B"
t20_release rollback --release-id "$T20_A"
t20_release status
```

Новые requests должны отражать текущий release/model. История и events volume
сохраняются. Старые approvals после изменения release/generation могут устареть;
нужны новые gate/package/check/approval, а не изменение JSON. Stop/start имеет
измеримый простой; нулевой downtime не обещается.

## Реальное восстановление

`t20_release reconcile` после обычного commit — idempotent consistency check,
не crash experiment. Для повторяемой реальной recovery проверки существует
документированный opt-in integration CLI: он выполняет Docker/HTTP/Registry
переходы и process-exit checkpoints, сохраняет journal и HTTP samples.

Подготовленные выше A/B/C/D пакеты используют лишь два demo fits. Создать fixture
по [release contract](releases.md): собственные recovery config/alias/port/network/
events volume, `releases.{A,B,C,D}.{package,check,release_id}`. A first-release,
B/C gated против A, D против B. Main demo и recovery не делят state directory.

```sh
T20_RECOVERY_REQUEST="$T20_ROOT/inputs/recovery-request.json"
T20_NEW_RECOVERY_EVIDENCE="$T20_ROOT/recovery-evidence"
python3 - "$T20_ROOT" "$T20_ID" "$T20_A" "$T20_B" "$T20_C" "$T20_D" <<'PY'
import json,sys
from pathlib import Path
root=Path(sys.argv[1]);name='ats-demo-recovery-'+sys.argv[2]
config=json.loads((root/'inputs/release-config.json').read_bytes())
config.update(project=name,port=13151,alias='recovery_'+sys.argv[2],events_volume=name+'-events',network=name+'-network')
releases={key:{'package':str(root/'releases'/release/'payload/package'),'check':str(root/'releases'/release/'check.json'),'release_id':release}
          for key,release in zip('ABCD',sys.argv[3:])}
with (root/'inputs/recovery-request.json').open('x') as f:json.dump({'config':config,'releases':releases},f,sort_keys=True);f.write('\n')
PY
PYTHONPATH=src:. ATS_RUN_RELEASE=1 ATS_RELEASE_REQUEST="$T20_RECOVERY_REQUEST" \
  ATS_RELEASE_EVIDENCE="$T20_NEW_RECOVERY_EVIDENCE" \
  python3 -B -m unittest tests.integration.test_release -v
```

Отсутствующий opt-in/skip не PASS. Все реальные negative outcomes сохраняются;
при unknown ready API или Registry drift оператор исследует своё состояние.
Ручной SIGKILL допустим только для собственного activate subprocess после
фактического durable pending. После сигнала дождаться завершения surviving
owned Docker effects и лишь затем выполнять reconcile; если commit опередил
сигнал, это `interruption_not_injected`. Продуктового crash flag нет.
Нельзя подделывать pending, удалять journal или убивать неизвестный сервис.

Для завершения показа остановить только свои ресурсы по сохранённому config.
Core `down` сохраняет named volumes; release events/state и evidence сохранить.
Не применять global prune, volume reset или default-state команды для очистки.
Demo preparation/recovery timing не выдаются за formal E2/E3 или качество найма.
