# Эксплуатация локального стенда

Первый проход нового читателя — [demo.md](demo.md): собственный runtime/state,
две заранее назначенные frozen demo модели, actual image/version identities,
публичные gate/package/check/release команды и реальное восстановление.
Архитектура и state stores описаны в [architecture.md](architecture.md),
принятые инженерные свидетельства — в [acceptance.md](acceptance.md).
Формальные измерения и human E2 отделены в [results.md](results.md).

| Контур | Публичный вход | Граница |
|---|---|---|
| Постоянный core | `scripts/bootstrap_core.py init/config/up/status` | Сохраняемые credentials/volumes; loopback, PostgreSQL без host port |
| DVC/ML | `python -m ats_mlops pipeline bundle/init/repro` | Readonly source, mutable workspace, admitted train/validation |
| Release | `python -m ats_mlops release --state-dir ... stage/approve/activate/status/rollback/reconcile` | Свой project/alias/port; A3 и durable journal |
| Airflow | `scripts/bootstrap_orchestration.py --project ... --core-env ...` | Explicit project должен совпасть с сохранённым core.env; image берётся из того же config |
| Технический мониторинг | `monitoring/bootstrap.py up/status` | Собственный профиль и read-only collection; не quality gate |

Команды ниже сохраняют историю принятых T4/T10/T13/T15 упражнений. Указанные в
их receipts номера моделей, source и времена не являются постоянным состоянием
нового backend. Текущее состояние своего release получают через `status`
и свежий `/metadata`, не по старому `/rank` replay. На общем host сначала
проверяется ownership; default core/Airflow/release не останавливают ради demo.

## Локальный постоянный core

Для новой отдельной среды `bootstrap_core.py init --project NAME --image-prefix
NAME --state-dir PRIVATE_CORE` сохраняет собственные build tags. Последующие
config/up/status используют этот config; другой prefix для уже созданного state
отвергается. Без нового параметра прежние defaults остаются совместимыми.
Core credentials хранить отдельно от ML workspaces и никогда не монтировать
весь private state в ML процесс. Полный пример scoped mounts — в [demo.md](demo.md).

Собственный Airflow подключается к этому core явно:

```sh
python3 -B scripts/bootstrap_orchestration.py init --project "$T20_PROJECT" --core-env "$T20_CORE/core.env" --state-dir "$T20_ROOT/airflow" --port 28150 --sanitized-git-source "$T20_REPO"
python3 -B scripts/bootstrap_orchestration.py build --project "$T20_PROJECT" --core-env "$T20_CORE/core.env" --state-dir "$T20_ROOT/airflow" --port 28150 --sanitized-git-source "$T20_REPO"
python3 -B scripts/bootstrap_orchestration.py up --project "$T20_PROJECT" --core-env "$T20_CORE/core.env" --state-dir "$T20_ROOT/airflow" --port 28150 --sanitized-git-source "$T20_REPO"
```

Не держать одновременно лишние ML/serving/Airflow workers при ограниченной памяти;
переходить к следующему этапу после проверки завершения собственного предыдущего.
Новый monitoring namespace этим изменением не добавляется.

Core содержит PostgreSQL 18.3, MinIO и MLflow 3.17.2/Registry. Версии ML и сервера MinIO закреплены в [versions.md](versions.md); T-4 добавляет официальный клиент [MinIO mc](https://github.com/minio/mc/tree/7394ce0dd2a80935aded936b09fa12cbb3cb8096), release `RELEASE.2025-08-13T08-35-41Z`. SHA-256 его исходного архива — `95cd293c7119f16921a6dc515a1fb74a2227f19fd994b9c8b770a154e802ac44`; Docker проверяет checksum и собирает клиент закреплённым Go image из T-2. Копия upstream лицензии включена в образ. Новый внешний сервис не требуется.

Из корня репозитория:

```sh
python3 scripts/bootstrap_core.py init
python3 scripts/bootstrap_core.py config
python3 scripts/bootstrap_core.py up
python3 scripts/bootstrap_core.py status
```

Первый `up` собирает образы; MinIO server/client собираются из upstream исходников с ограничением двух Go workers. ML venv и Airflow venv разделены, core использует только ML image. Docker должен иметь свободную память для лимитов core: PostgreSQL 512 MiB, MinIO 1 GiB, MLflow 2 GiB; это лимиты, не измеренное потребление. Первый успешный smoke показал MLflow 1.499 GiB при лимите 1.5 GiB, поэтому предел увеличен до 2 GiB и проверен повторно. При добавлении Airflow и observability нужно учитывать суммарный запас Docker VM; эти сервисы пока не входят в core.

Генератор создаёт случайные credentials в игнорируемом `.local/core/core.env` (0600), каталог имеет режим 0700. Повторный запуск сохраняет их. Не заменяйте файл при существующих volumes: PostgreSQL применяет init-скрипт только к пустому каталогу, а существующие пользователи должны сохранять прежние пароли. Для переноса стенда нужны и volumes, и приватная конфигурация. Не публикуйте `docker compose config`, `docker inspect`, полный env или MinIO root credentials: они доступны доверенному локальному владельцу Docker. Для проверки конфигурации используйте `config --quiet`.

Адреса по умолчанию: MLflow `http://127.0.0.1:5050`, MinIO S3 `http://127.0.0.1:9000`, console `http://127.0.0.1:9001`. Порты публикуются только на loopback, PostgreSQL не имеет host port. Перед первым `init` занятые порты можно заменить флагами `--minio-port`, `--console-port`, `--mlflow-port`; последующие команды используют сохранённые значения. Это локальный доверенный стенд без TLS/MLflow login; не открывайте порты во внешнюю сеть и не добавляйте публичный reverse proxy.

Для MLflow client достаточно источника `.local/core/mlflow.env` и tracking URI; артефакты проходят через MLflow proxy. Сервер получает только IAM identity для `ats-mlflow`. DVC identity имеет доступ только к `ats-dvc`, не получает MinIO admin или MLflow bucket. Root identity доступна MinIO и одноразовому `storage-init`, чья mc конфигурация находится в tmpfs. Используется [официальный policy attach](https://min.io/docs/minio/linux/reference/minio-mc-admin/mc-admin-policy-attach.html). PostgreSQL роли `mlflow`/`airflow` имеют отдельные БД, не являются superuser/createdb/createrole; PUBLIC connect отозван. Core bootstrap готовит БД Airflow; сам сервис подключается отдельной процедурой T10 ниже.

Когда T-3 metadata уже находится в checkout, настройте DVC client с установленным закреплённым ML окружением:

```sh
set -a
. .local/core/dvc.env
set +a
dvc remote add --local --default core s3://ats-dvc/data
dvc remote modify --local core endpointurl http://127.0.0.1:9000
dvc push
```

Секреты остаются в локальном env; `.dvc/config.local` игнорируется. В контейнерной сети endpoint — `http://minio:9000`; host loopback endpoint из контейнера использовать нельзя. `.local/core/client.json` хранит оба адреса и URI без credentials. Настройку remote не включают в публикуемые `.dvc/config` вместе с секретами.

```sh
python3 scripts/bootstrap_core.py restart
python3 scripts/bootstrap_core.py down
python3 scripts/bootstrap_core.py up
```

`restart` и `down` сохраняют named volumes `postgres-data`/`minio-data`. Команды с `down --volumes` удаляют данные: bootstrap CLI такой команды не предоставляет. Удаление volumes используется только в изолированном integration test с уникальным project name. Смена image не является миграцией схемы PostgreSQL; перед будущими обновлениями нужны backup и проверка восстановления в отдельном проекте.

Реальная проверка T-4:

```sh
ATS_RUN_STORAGE_TRACKING=1 python3 -m unittest discover -s tests/integration -p test_storage_tracking.py -v
```

Тест использует отдельный случайный project/порты/volumes и синтетическую fixture, выполняет настоящий DVC push, удаляет локальный payload/cache и восстанавливает pull; сохраняет run/artifact/зарегистрированную модель в MLflow с SQL backend; проверяет чтение после restart и `down`→`up` без удаления volumes. Дополнительно проверяются S3 cross-bucket read/write/list denials, неверные S3/SQL credentials, SQL роли/межбазовые запреты, loopback bindings и отказ MLflow для недоверенного Host. В finally удаляются только ресурсы этого тестового проекта, чужие контейнеры и образы не удаляются. Санитизированные evidence/logs и ресурсный snapshot сохраняются в `.local/core-tests/<run_id>/`; `skip` без opt-in не является прохождением. Тестовое время включает инфраструктуру и не является E-2 или временем обучения ATS.

Проверка на чистом commit `fd8677e7b36c0fdd1480d44b94184437c06e746a` завершилась **PASS**, все операции чтения/отказа и очистка подтверждены. Публикуемый [отчёт T-4](../reports/storage/t4.json) содержит hashes, точную model version и результаты. Снимок после проверок: MLflow 1.689 GiB из 2 GiB, MinIO 248.7 MiB, PostgreSQL 39.11 MiB; OOMKilled=false и RestartCount=0. Это один локальный snapshot, не peak/capacity benchmark. Исходные неудачные startup-прогоны и успешный прогон с прежним лимитом сохранены локально; публикуемый отчёт относится к повтору с 2 GiB. Bootstrap regression подтверждает восстановление недостающих client-файлов после прерванного init без ротации credentials и отказ для symlinks.

## Airflow: кандидат из допущенного bundle

Профиль `orchestration` добавляет один Airflow3.3.2 LocalExecutor с отдельным `/opt/ml-venv`; PostgreSQL/MinIO/MLflow остаются существующим `ats-core`. Запускайте последовательно, без `storage-check`, serving и monitoring: service limits core3.5GiB + Airflow3GiB =6.5GiB, DockerVM здесь7.75GiB, чужие контейнеры и overhead уменьшают остаток. Это бюджет по лимитам, не capacity benchmark. Airflow SQL использует существующую роль/БД airflow; one-shot migration заканчивается до training. Work, SQLite ledger, logs и auth сохраняются в отдельных named volumes. Redis и Docker socket отсутствуют.

```sh
python3 scripts/bootstrap_orchestration.py init
python3 scripts/bootstrap_orchestration.py config
python3 scripts/bootstrap_orchestration.py refresh-source
python3 scripts/bootstrap_orchestration.py build
python3 scripts/bootstrap_orchestration.py up
python3 scripts/bootstrap_orchestration.py status
# Останавливает только Airflow, volumes/core не удаляет:
python3 scripts/bootstrap_orchestration.py stop
```

Для другого checkout используйте `--core-env /absolute/path/to/existing/.local/core/core.env`; для выделенной приватной конфигурации — `--state-dir`, для свободного loopback port — `--port` (одинаковый на всех командах). Default source — фиксированный public GitHub repository. `--sanitized-git-source /path/to/repo` создаёт отдельный bare clone без working tree/ignored host secrets; в контейнер монтируется только этот clone. После нового source commit выполнить refresh-source. Образ и lock pinned, metadata image.json фиксируется после build; rebuild требует нового request payload. Private random API password `operator` хранится в `.local/orchestration/passwords.json`, fernet/JWT в private airflow.env. Не печатать resolved Compose/env, не передавать эти файлы в Git. UI/API по умолчанию `http://127.0.0.1:8080`; API token получается через POST `/auth/token`, DAG trigger — POST `/api/v2/dags/train_candidate/dagRuns`.

До DAG отдельно выполнить dataset admission (`pipeline bundle` для synthetic fixture) и upload **только** bundle descriptor + перечисленные train/validation/metadata files в `s3://ats-dvc/orchestration/bundles/<version>/`. DAG не генерирует корпус, не выполняет full admission и не читает raw test. Bundle inventory SHA берётся из `bundle.json.sha256`; dataset_version равен admitted generator config namespace. Клиент получает только DVC identity и MLflow proxy URI. SQL/auth/root/MLflow-bucket credentials не передаются ML subprocess.

Conf имеет exact поля `request_id`, `run_id`, `dataset_version`, `git_sha` (40 hex), `bundle_uri` (trusted S3 descriptor URI), `bundle_inventory_sha256`, `parameters`, `params_hash`. Parameters — объект `{"model":{},"search":{},"smoke":true,"evaluation":{"score_decimals":12}}`; params_hash — SHA256 UTF-8 `json.dumps(parameters,sort_keys=True,separators=(",",":"))`. Smoke2 маркирован; основная серия сохраняет20trials и n_jobs1. Request identity не равна Airflow dag_run_id. Source detached/clean/read-only, DVC workspace mutable, audited `pipeline.reproduce` проверяет source перед cache decision. XCom содержит только request/candidate/MLflow IDs и receipt/model URIs; outcome/counts/hashes/events находятся в `/work/requests/<request_id>/candidate.json` и repro receipts.

SQLite `/state/requests.sqlite3` атомарно связывает request с payload hash (включая runtime identity). Completed replay возвращает прежний candidate без DVC/refit/registration; другой payload с тем же request отвергается. Cache_hit использует проверенные exact исходные MLflow IDs, не создаёт новый run. DVC cache общий на work volume. Максимум3 orchestration attempts, бюджет сохраняется при clear/retrigger. Только typed transient download I/O повторяется; data/config/auth/missing object terminal. Durable intent предшествует DVC/fit. После сбоя восстанавливаются полностью записанные audited repro/candidate receipts; неоднозначный remote register→local receipt требует `recovery_required`, без автоматического refit и без обещания общей транзакции SQLite/MLflow. T10 создаёт кандидата; выпуск/alias не переключает.

Проверка выполняется на clean committed/pushed SHA. `python -m unittest discover -s tests/integration -p test_airflow_run.py` без opt-in сообщает skip, что не считается прохождением. Для реального gate запустить orchestration на порту8088 с state `.local/orchestration-t10`, затем:

```sh
ATS_RUN_AIRFLOW=1 ATS_AIRFLOW_PORT=8088 ATS_AIRFLOW_STATE="$PWD/.local/orchestration-t10" \
ATS_DVC_ENV_FILE=/absolute/path/to/existing/.local/core/dvc.env \
python3 -m unittest discover -s tests/integration -p test_airflow_run.py -v
```

Gate подготавливает/publishes уникальную synthetic admission fixture **до trigger**, затем через real API выполняет normal/replay/conflict/cache/transient/exhausted/fataldata. Private driver `faults.json` связывает request с количеством failed downloads: только эти worker attempts используют закрытый loopback endpoint вместо MinIO. Общие endpoints и core не меняются, fault не принимается из DAG conf; после gate файл очистить `{}`. Сохраняются SQLite attempt journal, task try_numbers/XCom, MLflow delta и before/after resource samples в private state/evidence-*; исходные failed runs сохраняются. Ручная проверка использует тот же API trigger и request-scoped fault plan. Service health/OOM/restarts проверяются отдельно; snapshots не peak/benchmark. При исчерпании памяти остановить только свой Airflow, не менять чужие контейнеры и лимит MLflow2GiB.

Финальная T10 проверка на clean/pushed executable source `9e83ec6fb69d6214e847ac093ebc92e56a0d4f61` прошла: pinned Airflow3.3.2 DAG import, **90 unit**, real API gate **89.950s**. [Отчёт T10](../reports/airflow/t10.json) связывает source/image/lock/bundle, exact candidates/model versions и raw evidence hash. Normal и transient дали суммарно **2 новых runs и 2 versions**; completed replay, cache, conflict, exhausted и invalid bundle не добавили runs/versions. Transient успешен на attempt2, exhausted остановился на3 и retrigger не добавил4; дефект bundle завершился на1 до repro intent/fit. XCom проверены через API, только IDs/URIs. API sample: Airflow connections9/max100. После gate Airflow1.077GiB/3GiB, MLflow1.719GiB/2GiB; core/Airflow healthy, OOM=false/restarts=0, чужие контейнеры не менялись. Это один ресурсный snapshot, без вывода о capacity/peak или основной серии20.

Ingress проверяется до загрузки partition bytes: descriptor inventory SHA → hash-verified manifest metadata → exact allowed set train/validation + fixed metadata/pointers, disjoint reserved artifact paths → bulk fetch. Лишний test/source/corpus/split object отвергается до S3 GET. Unit и независимый RecordingS3 boundary probe покрыли10 случаев, включая hidden alias, role overlap, traversal и повреждённый metadata hash; forbidden GET0. Это отдельное свидетельство границы входа, не замена реального API-gate.

Отдельная ручная API-проверка после gate: completed request успешно повторён на task try1; новый request с private one-shot bad endpoint завершён на try2 и `cache_hit`. Оба возвращают прежнюю exact model/run identity, MLflow run/version delta0; journal и XCom сохранены в отчёте. Fault plan после проверки очищен, shared endpoints не изменялись.

Независимое recovery свидетельство на том же source: в отдельной shadow copy завершённого request удалён только candidate receipt, ledger установлен в in_progress/attempt3. Реальный recovery child восстановил byte-identical candidate из existing audited repro receipt, завершил журнал и не вызвал DVC/download/fit/registration; четвёртой попытки нет. Shared runtime/ledger не менялись. Неоднозначный исход без полного receipt по-прежнему блокируется для оператора.

Предыдущие неудачи сохранены: pool2 migration timeout до DAG (pool увеличен до bounded5); fixture PATH без DVC до DAG; первый nonroot train дошёл до MLflow и остался `recovery_required` после cleanup read-only code copy. Последний не перезапускался: узкий T7 packaging helper создаёт temporary writable copy под basename `ats_mlops`, сверяет byte inventory и сохраняет исходный source неизменным. Реальный pinned MLflow copy/cleanup под UID50000 проверен без fit/remote calls; algorithms и lineage format сохранены. Рабочая политика остаётся conservative: неизвестный исход train/register не повторять автоматически.

## Local release operation (T13)

[Release lifecycle](releases.md) documents `release stage/approve/activate/rollback/reconcile/status`
and its immutable T11/T12 inputs. Default durable state is outside the checkout,
under `$XDG_STATE_HOME/ats-mlops` or `~/.local/state/ats-mlops`; the working event DB
is in a Linux named volume. Do not remove that volume or the stable serving
bridge during an update. Serving is loopback-only at port3000, with exact image
and readonly package mounts; Registry access stays in the host controller.

When an operation blocks with pending intent, restore access and run `reconcile`.
A healthy previous or target API remains available during Registry failure;
do not edit the journal, receipts or cached state. A stale candidate requires
new gate/package/check/approval against the current release ID and generation.
[T13 evidence](../reports/releases/t13.json) records actual crash recovery,
rollback, source identities and measured stop/start downtime. The historical
local demo initially used `t12-model4-59967b7` / model4 / champion4 with A3
`demo_only` approval; T15 below records its later snapshot, not every backend.

## Technical monitoring (T15)

[Monitoring operation](monitoring.md) provides the pinned local Prometheus/Grafana
profile, read-only collector and isolated acceptance protocol. Start or inspect it
with `python3 monitoring/bootstrap.py up|status`; Grafana Viewer is at
`http://127.0.0.1:3001/d/ats-technical`, Prometheus at `http://127.0.0.1:9090`.
Use the stable serving bridge; do not add host secrets or Docker sockets to
inference. A source read/snapshot mismatch sets source_up0 and suppresses stale
payload metrics. Keep the verified Airflow runtime override with its matching
identity so future bootstrap preserves the numeric snapshot hooks.

The saved T15 service snapshot is `t15-metrics-23145b9` / model4 / champion4,
generation2, with separate A3 approval and the same persistent events volume.
The earlier T13 release remains available for rollback. Working HTTP probes used
metadata only; the traffic/error fixture was backed up and owner-cleaned.
[T15 receipt](../reports/monitoring/technical.json) binds actual source/image,
Grafana frontend/query/UI proof, persistence, limits and cgroup diagnostics.
