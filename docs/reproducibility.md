# Локальный DVC DAG с immutable executable source

T9 использует `validate → prepare → train → evaluate` из `dvc.yaml`. Source и
mutable DVC workspace — соседние отдельные Git clones. Clean guard T7 сохраняется:
source остаётся на exact committed SHA, `dvc.lock`, данные, mutable params и
генерируемые outputs изменяются только в workspace. Кэш DVC и MinIO не выдаются за
новое обучение. Третий независимый пересчёт для E1 относится к T19.

## Admission до DAG

`pipeline init` принимает существующий локальный admitted bundle и его явный
inventory SHA. Он не генерирует данные и не проводит full admission. Bundle
содержит byte-identical manifest/admission receipt, actual train/validation/split
`.dvc` metadata, `dataset-binding.json` и только train/validation bytes.

Для первоначальной синтетической демонстрации есть отдельная команда в pinned ML
runtime, исполняющем source из readonly `/source/src`:

```sh
python -m ats_mlops pipeline bundle --source-root /source \
  --config /inputs/ci-config.json --output /inputs/admitted-bundle
```

Она выполняет T5 admission один раз во временном каталоге, сверяет actual DVC MD5,
size и manifest SHA256, затем сохраняет bundle без source/corpus/test/split raw
bytes. Descriptor `bundle.json` хеширует inventory остальных файлов; собственный
descriptor в этот inventory не входит. Последующий init/repro не запускает эту
команду. Новая dataset version требует нового admission/binding. CI namespace
отдельна от demo-v1. Произвольный feedback schema пока не поддержан T5 и не
объявляется поддержанным этим DAG.

## Init и audited repro

На host достаточно stdlib Python и Git; ML imports отложены до stages. Image JSON
содержит resolved `id`, reference, repo_digests, os и architecture из реального
`docker image inspect`. Clone source snapshot создаётся отдельно от workspace:

```sh
PYTHONPATH=src python -m ats_mlops pipeline init \
  --repo /repository --git-sha FULL_COMMITTED_SHA --directory /runs/example \
  --bundle /inputs/admitted-bundle --bundle-sha BUNDLE_INVENTORY_SHA \
  --image-identity /inputs/image.json
```

Смонтировать `/runs/example` как `/run`, отдельно readonly source как
`/run/source`, в существующий cached locked ML image. До первого импорта проекта:

```sh
export ATS_ML_PYTHON=/opt/ml-venv/bin/python
export PATH=/opt/ml-venv/bin:$PATH
export PYTHONPATH=/run/source/src
export PYTHONDONTWRITEBYTECODE=1
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export MLFLOW_TRACKING_URI=http://mlflow:5000
cd /run/workspace
dvc dag
dvc status --json
python -m ats_mlops pipeline repro --workspace . --source-root ../source
```

Старый `/opt/project/src` из cached image и workspace/src не используются.
Каждая стадия проверяет actual interpreter/module paths, clean source Git/tree,
source/runtime locks, platform/image identity, source recipe/config hashes и
admitted input metadata. `dvc.yaml` в workspace должен совпадать с source recipe.
`params.yaml` допускает явные model/search overrides, smoke и evaluation params.
Основной search budget остаётся 20; smoke задаётся отдельным bool и означает 2.

DVC запрещает `dvc.yaml` и `*.dvc` как file dependencies. Recipe и actual pointer
bytes поэтому привязаны declared `pipeline-inputs.json`/`dataset-binding.json`,
проверяются preflight/stages; сам DVC объявляет реальные train/validation byte
dependencies. Изменение admitted metadata требует нового bundle, а не отдельного
ручного редактирования pointer. Обёртка отвергает такую подмену до cache decision.

Validate сверяет actual workspace pointers с admitted binding и выполняет
training-mode `require_valid`. Prepare материализует возвращённые canonical
train/validation rows с сохранением manifest-relative paths, копирует неизменённые
manifest/receipt и сохраняет canonical configs. Эти outputs реально потребляются
следующими стадиями. Отдельного fitted preprocessing в prepare нет: TF-IDF fit
принадлежит единственному sklearn Pipeline и использует только train.

Train запускает T8 study в новом пустом staging/report directory; при успехе
отдельно вызывает T7 train с selected config, снова через admission guard.
Перезапись существующего stage output прямой командой отвергается. DVC управляет
удалением своих outs перед повторным выполнением; worker публикует новый staging
directory атомарно. Study не мутируется и не переиспользуется как новая попытка.
До tune/fit дополнительно сверяются exact prepared model/search/fit-params/feature
snapshot bytes с текущими effective params и manifest/admission hashes с binding.
Такая же проверка выполняется до evaluate; подмена даже другим valid bundle
не может незаметно дойти до fit.

Evaluate использует exact final model, общий scorer/evaluator и prepared rows.
Проверяет selected-config/data/model hashes, сохраняет scores/ranks, metrics,
vocabulary/IDF, transformed-feature hashes и `pipeline_manifest.json`.

## Cache-hit, provenance и ошибки

Обязательный операторский вход — тонкая обёртка `pipeline repro`: она выполняет
source/metadata preflight **до** решения DVC об использовании cache и сохраняет
machine-readable audit receipt. Низкоуровневый raw `dvc repro` сам по себе не
обещает этот preflight, если все commands пропущены из cache. `dvc dag/status`
остаются диагностикой. Обычный DVC вызывается внутри обёртки:

```sh
python -m ats_mlops pipeline repro --workspace . --source-root ../source
python -m ats_mlops pipeline repro --workspace . --source-root ../source --force
```

Второй вариант исполняет exact `dvc repro -f --no-run-cache`. Receipt в
`.local/repro/<invocation>.json` имеет outcome `new_training`, `cache_hit` или
`failed`, exit code, actual stage events и hashes финального lock/manifest.
Настоящие stages пишут execution events вне cached outs. Поэтому восстановление
удалённого model output из run cache правильно даёт cache_hit: сохранённый старый
run ID сам по себе не означает новый fit. Evaluation-only invalidation тоже даёт
training cache_hit и отдельный evaluate event. Ошибка остаётся failed.

`pipeline_manifest.json` отделяет source snapshot pointers T7 от authoritative
actual data pointers в `inputs.binding.workspace_pointers`. Он связывает
original/prepared manifest, receipt/binding, params snapshots, study, exact
T7 run/model version, model/report hashes и scope. Для новых CI данных старые
source demo pointers явно имеют роль `source_snapshot_only`. T7 manifest не
переписывается. Future gates должны учитывать составную pipeline lineage.

Порядок hashes: admitted inputs → study → T7 model/manifest → evaluation → pipeline
manifest. Final `dvc.lock` обновляется DVC после команды evaluate, поэтому его hash
хранится только во внешней post-repro receipt. Selfhash/lock-output cycle нет.

Source/config/data ошибки блокируются до fit; failed pending outputs сохраняются
в `.local/failed`, не публикуются как cached success. Failure event содержит safe
reason/error type и признак I/O retryability. Если train оборван после remote
MLflow commit до локального manifest/event, автоматический повтор небезопасен:
нужна проверка существующего remote run, а не предположение об отсутствии fit.
T9 не реализует Airflow retries/recovery — это T10.

Полный `dvc status --json` честно показывает намеренно отсутствующие standalone
raw source/corpus/test/split outputs у legacy `.dvc` stages. Это не причина читать
или восстанавливать reserved bytes. Отдельный `dvc status dvc.yaml --json`
показывает состояние **регулярного pipeline**; его нельзя называть полным status.
Оба результата и `dvc dag` сохраняются actual integration.

## Проверки

```sh
python -m unittest discover -s tests/unit -p test_pipeline.py
python -m unittest discover -s tests/unit
ATS_RUN_RECOMPUTE=1 ATS_RECOMPUTE_EVIDENCE_DIR=/absolute/external/evidence \
  ATS_DVC_ENV_FILE=/private/path/to/dvc.env \
  python -m unittest discover -s tests/integration -p test_recompute.py
```

Integration запускается на Docker host. Она использует cached image и existing
`ats-core_default`/MLflow/MinIO, без build/новых services. Credentials передаются
только selective DVC transfer container через env-file и не печатаются. Один CI
bundle проходит admission до DAG; две fresh source/workspace пары восстанавливают
только train/validation через реальный DVC/MinIO. В каждой exact forced command
создаёт study20 и отдельный новый final MLflow run/version. Prepared hashes,
vocabulary/IDF/features и ranks сравниваются; scores/metrics используют
`atol=1e-6,rtol=1e-5`. Binary model equality не требуется.

Проверяются unchanged cache-hit, output-deleted run-cache restore (оба без новых
MLflow runs), evaluation-only и model-param invalidation, invalid data до fit.
Model-param проверка создаёт дополнительный обычный run в существующем workspace;
это не третий независимый forced recompute. Skip не является PASS. Все свидетельства
остаются synthetic `demo_only`: они не подтверждают качество реального найма.

## Actual gate, 2026-10-26

Исполняемый source `0601a5ae2d6728a0ebad3c4bbd5829a7254186d2` был clean и опубликован
до actual gate. Поздний generated-lock/docs/evidence commit не заменяет этот SHA.
Полная unit suite: 79 PASS, включая 11 pipeline cases; actual integration: 1 PASS
за 76.344 s. Cached runtime image:
`sha256:574c48deb107cba3f0bdc982d3aa0ab1500de58ab1694052bb92efbcab13ec91`.

Две fresh пары выполнили exact `dvc repro -f --no-run-cache`, study20 и отдельный
final fit: `models:/ats-pair-ranking/4` (run
`418211bd1ce84cf1bdbbef4f372b1fb4`) и `/5` (run
`d267ecd265bc405a8d6497865823e883`). Prepared hashes, vocabulary/IDF, transformed
feature hashes и ranks совпали; scores/metrics прошли `atol=1e-6,rtol=1e-5`.
Unchanged, удалённый training-output restore и evaluation-only change дали
cache_hit без новых runs. Обычное изменение model max_iter создало version6;
invalid data затем завершились failed без нового fit/run. Current workspace A
оставлен с этим отрицательным входом; `snapshots/a` хранит первый successful run.

Первый attempt на `b53b032` остановился до stages/fit из-за запрета DVC-file deps.
Эта декларация исправлена; независимо найденная prepared-config подмена также
закрыта before-fit checks и regression fit spies = 0 до окончательного gate.

Сохранены [checks](../reports/recompute/checks.json),
[полная квитанция integration](../reports/recompute/evidence.json),
[A manifest](../reports/recompute/a/pipeline_manifest.json),
[B manifest](../reports/recompute/b/pipeline_manifest.json),
оба locks/dags/statuses/studies и score/metric reports.
Публичные ASCII DAG copies имеют удалённый правый whitespace padding; исходный
stdout сохранён в external evidence.
Полные private model/data artifacts, invocation logs и failed-recipe attempt
сохранены оператором во внешнем evidence directory, переданном через
`ATS_RECOMPUTE_EVIDENCE_DIR`. Результаты моделей также доступны
через exact persistent Registry versions. Model downloads/caches/raw data в Git
не добавлены. Сохранённый root `dvc.lock` взят из successful workspace B.

## Frozen parameters для retraining / E2

В обычном `params.yaml` можно выбрать `"search": {"mode": "frozen"}` при
`"smoke": false`. Остальные top-level keys остаются `model/search/smoke/evaluation`.
Effective model config получается из закреплённого `configs/model.json` и явных
`params.model` overrides, проверяется и сохраняется в prepared `model.json`.
Frozen mode запрещает search ranges/budget overrides и smoke; неизвестный mode
отклоняется до stage outputs или orchestration side effects.

Train в этом режиме не вызывает Optuna и обучает один обычный Pipeline с точным
prepared config. До train создаётся отдельный `training/selection.json`:
`kind=frozen`, `outcome=selected`, `new_trials=0`, config/hash, effective-fit и
prepared inventories, manifest/admission hashes, source Git/tree и image/lock.
`study/` и фиктивного successful study нет. Evaluation embedding
`pipeline_manifest.selection` связывает receipt bytes с actual run/model;
T11 проверяет exact params, prepared/data/source/runtime bindings и zero trials.
`selection` и `study` одновременно запрещены.

Без mode сохраняется прежний Optuna path:20 configured trials либо2 в smoke,
прежняя study lineage и historical manifests не переписываются. DVC source,
prepared bytes и params dependencies сохраняют обычные cache semantics; cache hit
не выдаётся за новое вычисление и не используется как замена HPO-off. Изменение
одного `evaluation.score_decimals` по-прежнему требует evaluation, не нового fit:
selection receipt связывает effective fit snapshot, а не полный mutable params file.

Это инженерный execution option. Он не подтверждает результаты E2/E5; actual
clean frozen candidate и T11 evidence выполняются отдельно после code freeze и
согласования resource window. Policy/outbox и T16 adapters подключаются отдельно.
