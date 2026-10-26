# T12: автономный Bento API и frozen release package

`service/` намеренно не входит в `ats_mlops`: в fresh isolated Python сначала
загружается код exact MLflow artifact, затем его scoring/features helpers.
Generic image не содержит модели или release manifest. Модельный gate и release
check являются разными проверками; approve/activate принадлежат T13.

## Упаковка

После clean commit/push serving source построить image из cached pinned base:

```sh
test "$(docker image inspect --format '{{.Id}}' ats-core-ml:t4)" = sha256:574c48deb107cba3f0bdc982d3aa0ab1500de58ab1694052bb92efbcab13ec91
docker build --pull=false --network=none -f docker/serving/Dockerfile --build-arg SOURCE_SHA="$(git rev-parse HEAD)" -t ats-serving:t12 .
```

Не добавляются зависимости, registry или службы. Получить resolved image ID,
OS/architecture и label revision точечным `docker image inspect --format`.
В isolated одноразовом контейнере этого image получить `service.runtime.environment()`;
serving identity JSON содержит `image:{id,os,architecture}`, `git_sha`,
`source_sha256` (strict inventory `service/`) и `environment`.
Training environment из run manifest хранится отдельно: Python patch 3.11.16
Airflow и 3.11.17 core не объявляются одинаковыми.

Package request: `release_id`, paths `gate_report`, `run_manifest`,
`pipeline_manifest`, `serving_identity`. В существующем locked ML runtime:

```sh
python -B -m service.package --request /private/package-request.json --output /private/releases/release-001
```

Требуется существующий MLflow tracking access только при import exact numeric
URI из passed gate. Alias/latest не принимаются. Local tag использует `m` и
первые60 hex model SHA, потому что Bento1.4.40 ограничивает version63 символами;
полный SHA и bytes inventory проверяются независимо. Никакого unpickle при
packaging. Результат: frozen manifest, gate/run/pipeline sidecars и exact local
Bento store. Existing output не перезаписывается. Manifest не содержит будущий
release-check, собственный hash, approval или mutable state.

## Запуск и контракт

`docker/serving/compose.yaml` требует `ATS_SERVING_IMAGE` (resolved ID),
`ATS_RELEASE_PACKAGE` и optional `ATS_SERVING_PORT`. Package `/release:ro`,
`models/` readonly submount, `/runtime` и `/tmp` ephemeral; `/events` persistent.
Root FS readonly, non-root UID50000, host port loopback. Контейнеру не нужны
Registry/S3 credentials, training checkout или data mounts.

`POST /rank`: `{request_id,vacancy:{id,text},candidates:[{id,text}],top_k}`.
Ответ: `{request_id,model_version,release_id,ranking:[{candidate_id,score}]}`.
Tie — candidate ID ascending. IDs: opaque ASCII `[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}`.
0..100 candidates, nonempty normalized text≤10000characters, request≤1MiB.
Превышение размера —413; schema/duplicate IDs/strict integer top_k1..100 —422.
Empty candidates записывает пустой full event и возвращает пустой ranking.
Model unavailable или SQLite failure —503. ASGI boundary проверяет input до
SDK, чтобы Bento default400/traceback не раскрывал приватный текст.

`POST /metadata` с `{}`/пустым body возвращает readiness и точные hashes/IDs,
scope и runtime receipt; не training data. Metadata image declaration проверяется
host coordinator против actual container `.Image`.

SQLite сохраняет full ordered scores и пять numeric features до top_k в одной
FULL-sync transaction с original response. Primary-key request ID и canonical
input hash включают top_k. Same ID/hash возвращает original model/release/response
даже после restart; другой input —422. Failed transaction не даёт успешный ответ.
Сырые тексты/контакты в DB/logs не сохраняются.

## Actual release check

Staging request: `package`, `validation_manifest`, `admission_report`,
`dataset_binding`, `alias_other_version` (другая уже существующая numeric version).
Host environment `PYTHONPATH=src` используется только coordinator, не model worker.

```sh
PYTHONPATH=src:. ATS_RUN_RANK_API=1 ATS_T12_REQUEST=/absolute/private/stage-request.json \
  ATS_T12_EVIDENCE_DIR=/absolute/private/evidence \
  python -B -m unittest discover -s tests/integration -p test_rank_api.py
```

Check заново получает native reference в recorded training image и сверяет его
score hash с passed model gate, затем проверяет HTTP scores всех gate-bound
validation groups в actual generic serving image (atol1e-12). Test не читается.
Проверяются readonly/code isolation, schema/size/empty/ties/privacy, full events,
retry/restart/conflict, missing model и event-write503. В Registry перемещается
только уникальный temporary fixture alias между существующими versions, затем
он удаляется; candidate/champion не меняются. Staging DB — отдельная временная
директория, уничтожается после проверки, не входит в drift inputs.

Report exclusive-write, содержит outcome/scope/model/Bento/image/gate/manifest
identities, actual named checks и unchanged manifest SHA. Report пишется вне
package и не добавляется обратно в manifest. Skip не является PASS. Текущие
actual результаты публикуются отдельным evidence commit после executable source.
