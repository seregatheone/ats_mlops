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
model store `/models:ro`, `/runtime` и `/tmp` ephemeral; `/events` persistent.
Launcher создаёт owned writable Bento home и `models` symlink на readonly mount:
Docker nested-bind parents иначе принадлежат root и мешают non-root bootstrap.
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
он удаляется в finally; candidate/champion не меняются. Staging DB — отдельный
временный Docker named volume, уничтожается после проверки, не входит в drift
inputs. Live SQL/locks и checkpoint выполняются только в Linux, не через macOS
host bind: межсистемные WAL/byte-lock semantics не являются надёжной проверкой.
После остановки выполняется WAL checkpoint и immutable readonly export.

Report exclusive-write, содержит outcome/scope/model/Bento/image/gate/manifest
identities, actual named checks и unchanged manifest SHA. Report пишется вне
package и не добавляется обратно в manifest. Skip не является PASS. Текущие
actual результаты публикуются отдельным evidence commit после executable source.

## Actual evidence, 2026-10-26

Executable source заранее committed/pushed:
`59967b72f963fd86472688c785c466ca4cd47f52`. Последующий evidence commit не
заменяет этот build/source SHA. Generic image:
`sha256:6123886de312aeea6c0dd0fc2dfd2463bad7a4913c2f0fc76a93cfa51a8b6cf7`.

Exact model `models:/ats-pair-ranking/4` импортирован через BentoML1.4.40 без
обучения/новой Registry version. [Frozen release manifest](../reports/serving/release_manifest.json)
связывает model tree/payload/code, gate SHA, API schema и serving image. Его SHA256
`6dacc847605267c917c27b32aaf81c23d5473652ca562dc3d4cc47230340ac1e`
до/после actual check одинаков. Model weights/store и исходные тексты остаются в
приватном package/evidence вне Git; опубликованы только текстовые identities.

[Release check](../reports/serving/release_check_report.json) — **passed + demo_only**,
все12 named checks выполнены. HTTP scores всех12 validation groups/240пар совпали
с fresh native reference в recorded training image точно (max difference0).
Reference scores SHA совпал с прошедшим T11 model gate. Проверены fresh source-free
load, readonly package/store, actual image ID и OCI source revision; schema/size
(включая actual1MiB/+1 и chunked overflow),100/101candidates, Unicode10k text,
empty/ties, full events до top_k, retry/conflict/restart, corrupt replay503,
Linux SQLite lock503/no partial event, missing model503 и original-release replay.
Fixture-only alias реально перемещён4→5; **новые request IDs** на каждом положении
подтвердили fresh inference неизменной local модели, alias удалён. Sentinel
приватного текста/контакта отсутствует в technical logs/events; staging volume и
контейнеры удалены.

[Обычный Compose smoke](../reports/serving/compose-smoke.json) стартовал с новым
named volume от UID50000 без ручного chown: metadata ready=true, rank200; после
проверки project/network/volume удалены. Ownership `/events` задаёт image.
Full canonical unit suite на том же source —121PASS; focused stdlib —8PASS.
[Checks и hashes](../reports/serving/checks.json) фиксируют также предыдущие
rejected попытки и причины исправлений; они не выдаются за successful evidence.

Этот model4 run использует training и serving Python3.11.17. Environments
записаны раздельно; код не требует универсального совпадения patch. Airflow3.11.16
candidate обязан пройти собственные model gate и actual serving check; текущий
результат не выдаётся за проверку такого другого candidate. Проверка синтетическая
`demo_only`, без вывода о реальном найме; approve/activate ещё не выполнялись.
