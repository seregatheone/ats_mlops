# T17: свидетельство замкнутого цикла

Actual002 прошёл на executable source `48b6d3adc6853971d583f8cc3c93e4aebc4bac00`.
Этот commit не переатрибутируется последующему docs/evidence commit. Public
[summary](../reports/retraining/t17.json) содержит exact IDs, raw/public SHA и
71 переносимый backing file с metadata/numeric bytes. Независимое final review,
CI и merge учитываются отдельно; T17 остаётся unchecked до merge.

Начальная модель13 была квалифицирована раньше на source
`a803dad354aa903038b80b20b6a0ba974657a3e9`. Она была actual ACTIVE с epoch3 при
свежей проверке T13. Это повторно использованная квалификация incumbent/reference,
а не новый fit T17. Новый обычный candidate имеет следующие координаты:

| Поле | Actual значение |
|---|---|
| Request | `retrain-e7e94cf2c563eb93f0f3444cafe73b50b84953710f7c1b8d` |
| Airflow run | `retrain__e7e94cf2c563eb93f0f3444cafe73b50b84953710f7c1b8dbd3c2aea368e1a1a` |
| MLflow | `models:/ats-pair-ranking/14` |
| Training run | `a37686ddac5f44b0847f402ff8ab911f` |
| Model tree SHA | `5ccf380b8f8a1a60401682906eef6094be3c40c1a9033616326073ec91b5bd66` |
| Release | `retrain-e7e94cf2c563eb93f0f3444cafe73b50b84953710f7c1b8d-r1` |
| ACTIVE generation | `4` |
| Reference SHA | `3d6a43aa613edcbc3a398c8dadc78aea657893457444c9ba7cce3d96df287965` |
| Effective rule SHA | `5e4588ce18df08599cbb9415f8b45364936d0d0d079c65c7cc6aae7098afa8df` |

Два соседних реальных окна по240пар/12вакансий дали drift=true и одну заявку
waiting_for_labels. Прогнозы сохранились полными группами в отдельном native event
volume. Меток в исходном feedback snapshot не было; качество осталось undefined.
Оба exact receipt повторно прошли через реальные monitor DAG deliveries без
добавления заявок/моделей. См. [cohort](../reports/retraining/t17/phases/cohort.json)
и [replay](../reports/retraining/t17/phases/receipt-replay.json).

Далее был одобрен genuine schema2 dataset v2 с заранее фиксированным seed20261027,
GX admission и explicit reservation parent. Semantic pair+label hash изменился,
new_label_count=960; новый namespace сам по себе не считался новой разметкой.
Это независимый synthetic teacher, а не человеческая оценка и не model truth.
Исходные canonical test bodies не открывались. Однократная admission проверяла
свежесгенерированные reserved rows до train/validation-only export.

Реальный caller завершился с exit86 после accepted T10 POST и до native ACK.
Сохранённый [marker](../reports/retraining/t17/phases/accepted-post-before-ack.json)
связывает exact run/conf/outbox. Перед восстановлением реальный DAG уже был
success, native request ещё queued; fresh process выполнил GET того же run/conf
и продолжил исходный outbox. Task clear, новый run ID и refit не использовались.
Registry delta составил одну version и один training run.

Native [repro receipt](../reports/retraining/t17/native-training/repro.json)
содержит одну обычную команду `dvc repro`, outcome=new_training и выполненные
validate/prepare/train/evaluate. Один recorded final train event связан с14/run
выше; [selection](../reports/retraining/t17/lineage/selection.json) фиксирует
frozen mode,0new trials, C1/max_features2000/model seed20261026. Две сохранённые
repro-копии семантически равны, но formatter bytes различаются; их SHA указаны
отдельно. Это не forced E1 replica и не E2 measurement.

Actual T11 validation-only gate сравнил candidate с baseline и incumbent13 на
одном v2 cohort. T12 package/check проверил API scores, immutable manifest,
restart/replay/idempotency/privacy и source/image bindings. Candidate сначала
ждал waiting_approval. До A3 были выполнены artifact-own GX/combined reservation
guard, feature worker и новая provider calibration с тем же effective rule.
Полный sanitized [training guard](../reports/retraining/t17/references/candidate/data-training-guard.json)
сохранён точными bytes, а не только digest.

Отдельный authorized agent-operator выполнил штатный T13 approve/activate после
ready-проверок. [A3 receipt](../reports/retraining/t17/operator/A3.json) связывает
generation4 и journal SHA `ff4f7260e7c9a517e681e4b5fa640e73f56d8b367a31d499ab02255ce31d7e30`.
Затем fresh ACTIVE verification и native intent/reconcile обновили reference
cursor. Два свежих реальных окна model14 по220пар/11вакансий снова были сигнальными,
но дали cooldown, requests=1/inflight=None и0новых моделей. Working events
измерены до/после и остались0. Политика сама не approve/activate.

Training/serving/provider environments не объявлены одинаковыми: native Airflow
training использовал Python3.11.16/ML lock; serving и provider — Python3.11.17.
Provider использовал отдельный Pandas2 lock. Exact image IDs, packages, source
hashes, native scoring receipts и actual API comparison сохранены в summary и
backing files; разница Python patch не скрыта.

Отрицательный887 attempt сохранён отдельно: provider window0 был построен, затем
macOS отказал в snapshot rename после раннего chmod0555. Native window/fit не
создались. Source48 оставляет top staging writable до rename и сразу закрывает
published target0555. На887 прошли238 canonical unit tests; fix48 отдельно прошёл
9 focused macOS tests и последующий actual002. Старый provider receipt не
переатрибутирован source48.

Реальные transport/replay/cooldown probes отделены от структурных hash/count
проверок. Bad-candidate, race/backward-clock и storage-negative branches покрыты
policy fixtures и предыдущими T11/T13 native receipts; actual002 обучил одного
хорошего кандидата. Public metadata/numeric bytes копируются точно, без model
binaries, raw texts, credentials и personal paths. Provider HTML сохранён
приватно с SHA; native verifier проверил его при исполнении.

Формальные E1–E5 не выполнялись этим свидетельством: нет трёх forced replicas,
пяти paired manual/auto измерений, повторной серии отказов, полного seeded E4
или финальной test evaluation. E5 ledger/scoring и исходный test не использованы.
Synthetic demo не доказывает улучшение реального найма.
