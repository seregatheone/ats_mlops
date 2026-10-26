# Progress: реализация диплома по автоматизации жизненного цикла ML в ATS

> Plan: ./plan.md · Tasks: ./tasks.md. Планирование не означает выполнения задач реализации.

## Status
- [x] T-1 — Постановка и протокол
- [x] T-2 — Каркас и совместимость
- [x] T-3 — Данные и разбиение
- [x] T-4 — MinIO и MLflow
- [x] T-5 — Качество данных
- [x] T-6 — Baseline и метрики
- [x] T-7 — Модель и происхождение
- [x] T-8 — Optuna
- [x] T-9 — DVC DAG
- [x] T-10 — Airflow
- [x] T-11 — Gate кандидата
- [x] T-12 — BentoML API
- [x] T-13 — Выпуск и откат
- [x] T-14 — GitHub Actions
- [x] T-15 — Prometheus/Grafana
- [x] T-16 — Evidently и обратная связь
- [ ] T-17 — Переобучение (actual PASS; final review/CI/merge pending)
- [ ] T-18 — Приёмка
- [ ] T-19 — Эксперименты
- [ ] T-20 — Диплом и демонстрация

## Learnings
<!-- Добавлять наблюдения по мере выполнения задач. -->

- T-17: executable48b6d3a прошёл actual native loop13/epoch3→две signal windows→одна waiting request→approved genuine v2(seed20261027)→обычный frozen Airflow candidate14→T11/T12→отдельный A3/epoch4→reference reconcile→fresh cooldown без новой заявки. Реальный accepted POST/exit86 восстановлен fresh process по исходному run/outbox, exact receipts повторены через реальные DAG. Native repro содержит один final train event, Registry1version/1run,0trials; семантически равные repro JSON имеют разные formatter bytes. Failed887 Mac snapshot publish и narrowfix атрибутированы отдельно. Public metadata/numeric proofs и hashes: reports/retraining/t17.json; E1–E5/исходныйtest/actualE5ledger не выполнялись. Checkbox остаётся до final review/CI/merge.

- T-14: изолированный GitHub-hosted ARM CI выполняет static/unit и synthetic lifecycle с временными MinIO/MLflow, DVC/Optuna/gate/Bento/API/release. Genuine negative40260037093 отклонил fixture99; восстановленный code2292065 прошёл145unit/11lifecycle/12API, maxdiff0. Docs head15216ff отдельно прошёл Actions40263217470. Подтверждённые Linux UID/private-dir/package-mode дефекты исправлены без ослабления hash/isolation guards; пустая/неполная/неверно типизированная fixture отвергается. Локальная серия1b6 и последующие targeted fixes атрибутированы отдельно; reports/ci хранит исходные failed/pass receipts. Airflow recovery и production deploy в smoke не выполняются.
- T-15: requests/errors/latency с bounded labels и read-only pipeline/release monitoring доступны в pinned Prometheus/Grafana. Canonical152unit PASS; actual004 девять проверок PASS36.845s, включая200/422/503, privacy, real ledger/journal, Grafana query и restart persistence; root подтвердил UI. Idle-WAL snapshot fallback и Grafana bundled-plugin/memory defects исправлены с отдельными receipts. Default model4/champion4 обновлён через T13/A3 без локального fit и working prediction events; fixture очищен, monitoring/core/Airflow оставлены healthy. Компонентные SHA и ресурсные измерения: reports/monitoring/technical.json.

- T-13: контроллер проверяет immutable gates/approval и current release под lock; journal payload checksums защищают tail и generation guard. На executable5fd94b0 прошли135 unit, syntax70py и actual18checks с process crashes, stale C/ABA, rollback и combined container/Registry failure. Независимая приёмка PASS, оба semantic findings закрыты. API восстанавливается локально до согласования alias при outage. Reports/releases/t13.json хранит raw hashes и monotonic outage intervals. Постоянный demo_only release модели4 активен на loopback3000 по A3; champion обновлён после ready, рабочий журнал событий пуст. После executed source — только docs/EOF whitespace.

- T-12: exact MLflow version4 упакована в Bento, manifest заморожен до staging. На executable59967b7 прошли121 unit, actual12checks и Compose smoke с новым volume; все240 HTTP scores совпали с native reference. Независимая приёмка VERIFIED: контракт, SQLite replay/concurrency/corrupt rows, model/source/image/report binding и actual privacy. Manifest6dacc847 не изменился. Журнал хранится на native Docker volume; macOS bind mount не подходит для проверки SQLite WAL/locks. Отчёты reports/serving; approval/activation относятся к T-13, scope demo_only.

- T-11: modelgate связывает exactmodel/pipeline/authoritative-data hashes с validation-onlycohort, NDCGdrop≤0.005 противbaseline/incumbent, outcome/scope разделены. Firstrelease и fixtureincumbent прошли actualisolated artifact gates на240rows; bytecode/cross-artifact rejected. Оба P2 metadataTOCTOU/outputrace исправлены; независимаяacceptance VERIFIED, 102unit наfixsource и113 integratedunitPASS послеAirflowbase. Qualityreport неapprove/activate; incumbent recheck принадлежитT13.

- T-10: Airflow3.3.2 LocalExecutor вызывает отдельный locked ML venv/immutable source и admittedbundle; SQLiteledger сохраняет idempotency/retrybudget. 90 unit, DAGimport, actual APIgate89.950s и independentacceptance VERIFIED; normal/transient2runs, replay/cache/conflict/fatal/exhausted0extra. XCom только5IDs/URIs, noOOM/restarts. Исправлены nonroot readonly code packaging, childmanifest replaybinding, attempt3recovery и forbiddeningress. Report reports/airflow/t10.json; ambiguouspostregister остаётся recovery_required, не exactly-onceclaim.

- T-9: audited pipeline repro проверяет immutable source/bundle до cache; DVC выполняет validate/prepare/train/evaluate. Два fresh forced main20 дали разные run/version4/5 и совпадающие prepared/features/vocab/IDF/ranks/scores/metrics; cache/restore/eval-only создали0runs. 79 unit, actualintegration76.344s и независимая R1приёмка VERIFIED. Preparedsnapshot P2 исправлен доfit. Fullstatus честно показывает4missing reserved standalone outputs, pipeline-status clean; rawcachedDVC metadatapreflight не обещан. Reports/recompute и docs/reproducibility.md сохраняют границы и executable0601a5ae.

- T-8: seeded sequential Optuna выполнила smoke 2 и основную серию 20 trials; все COMPLETE/NDCG1.0 на demo_only validation, tie выбирает trial0. Selected C1/max_features2000 применены отдельным final training run, exact Registry version3. Immutable study→selected config→final receipt hashes и live MLflow binding проверены; 68 accumulated unit PASS. Trial models не регистрируются, test не использовался, улучшение реального найма не заявляется.

- T-7: Pipeline fit только на проверенных in-memory train rows; реальный guard блокирует Pipeline/LR fit на плохом входе. MLflow exact version ats-pair-ranking/2 и lineage проверены; native/pyfunc в отдельных isolated контейнерах без checkout дали 240 совпадающих scores. 57 unit, 24 независимых model checks и 31 provenance/portability checks PASS. Удаление code ломает fresh load; reports/training и docs/training.md сохраняют actual source SHA 5823d425, demo_only.

- T-5: real GX admission проверяет source до dedup; training preflight возвращает точные проверенные train/validation rows и требует полный bound receipt. JSON/Markdown без текстов/PII; 35 unit и независимый installed-wheel positive/negative smoke PASS. Исправлены два P2: manifest TOCTOU и недоступные после установки GX configs. Реальный fit guard проверяется в T-7, T-5 его выполнение не заявляет.

- T-6: Jaccard и общий NDCG@10/Recall@10 evaluator проверены ручным примером и независимым oracle. Zero-positive/empty/undefined/ties/invalid scores покрыты; baseline читает только train/validation. 32 unit и 35 независимых checks PASS в Python 3.11, validation report: 12 групп, обе метрики 1.0. Это упрощённая синтетика demo_only, не качество найма.

- T-4: persistent core сохраняет DVC data, run/artifact/Registry model после restart и down/up; S3 bucket/SQL role/Host negative probes прошли. Реальный smoke 49.032 s, cleanup 0; независимая приёмка VERIFIED. MLflow limit 2 GiB после измерения, warm sample 1.689 GiB, OOM/restarts 0; это не capacity benchmark. Свидетельства docs/runbook.md и reports/storage/t4.json.

- T-3: 1 200 синтетических пар/60 компонентов, split 36/12/12 групп; source/config/test hashes закреплены. Шесть настоящих DVC pointers, восстановление и чистый offline checkout проверены. Независимые 18 unit и 23 adversarial проверки прошли в Python 3.11. Scoped loader train/validation не читает test; свидетельства docs/data-card.md и reports/data-audit.json.

- T-2: MinIO community собран из проверенного source commit; PostgreSQL init включён в image, Airflow auth store требует writable внутреннего volume. Реальный LocalExecutor/GX/DVC/MLflow/Bento smoke и cleanup прошли; отчёт reports/compatibility/t2.json. Отрицательный lock guard дополнительно проверен реальной Git unit fixture.

## Execution

- Epic #1; T-N corresponds to issue N+1.
- T-1 input decisions and the single README bootstrap approved on 26.10.2026.
- T-1 documents accepted and checked: R-1–R-8/E-1–E-5 coverage, relative links, authorization evidence and absence of private paths; publication through the issue-2 PR.
