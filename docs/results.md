# Экспериментальные результаты: заготовка T19

Статус: **implementation-only, E1–E5 not_run**. Этот файл не является отчётом
проведённого исследования. Unit-тесты harness используют только синтетические
временные данные и не учитываются как экспериментальные прогоны. Исходный test,
ML-артефакты и реальные сервисы при подготовке harness не открывались.

| Серия | Статус | Требуемые исходные свидетельства |
|---|---|---|
| E1 | `not_run` | Ровно3 fresh forced final fits,3 новых runs/версии и сравнение numeric raws |
| E2 | `not_run` | Ровно5 человеческих manual/auto пар, журнал/attestation, порядок и кеш |
| E3 | `not_run` | Шесть видов отказа ×3, собственные ресурсы, detection/recovery/HTTP samples |
| E4 | `not_run` | T17 receipts для контрольных/сигнальных/повторных окон и поступления меток |
| E5 | `not_run` | Один защищённый test read/scoring, обе модели, numeric commit и native guard |

Методика сохраняет [протокол](experiment-protocol.md). Новый исполняемый код и
статистический rebuild должны быть committed до T18 freeze. После этого T18
проверяет восемь реальных сценариев и создаёт отдельный freeze receipt с внешним
ожидаемым SHA. Model source SHA и source SHA стенда разделены. Текущий clean13 —
ранний инженерный артефакт, не независимый E1-пересчёт. Старые4/5 загрязнены
историческим split и не являются допустимыми кандидатами E5.

## Подготовка спецификации и ограничения исполнения

`configs/experiment.template.json` — авторитетная версия schema1. Nulls означают
неготовые входы/адаптеры, а не разрешение использовать default. `validate_spec`
без `template=True` отвергает незаполненную спецификацию. В ней заранее заданы
frozen HPO-off/C1/max_features2000, data seed20261027/model seed20261026/threads1,
E4 seed20261029 отдельно от calibration1707, bootstrap2000/seed20261030/min10groups.
`environment_sha256` должен связывать фактические image/platform/Python/lock,
ресурсы и threads через closure, которую проверяет T18. `bindings` содержат
хеши metadata; только наличие hash-строки не доказывает доступность или допуск.

Подготовительный CLI не выполняет эффекты и не имеет force/reset:

```sh
PYTHONPATH=src python3 -B -m ats_mlops.evaluation.experiment validate-spec \
  configs/experiment.template.json --template
PYTHONPATH=src python3 -B -m ats_mlops.evaluation.experiment summarize /private/study
PYTHONPATH=src python3 -B -m ats_mlops.evaluation.experiment rebuild \
  /private/study/E5/numeric.json /private/rebuilt
```

Ни один production adapter пока не объявлен реализованным. `adapter_bindings`
должны быть заполнены точными entrypoint/source SHA, а существующие audited
DVC/T10/T11/T12/T13/T17 команды реально подключены и приняты **до final freeze**.
До этого допускаются только toy callback unit tests. Global CLI не изменяется.
Нельзя получить научные прогоны из JSON с `outcome: passed` без проверки native
происхождения его наблюдений в конкретном адаптере и T18 closure.

## Интерфейсы harness и квитанции

`initialize(spec_path, output, freeze_path, expected_freeze_sha256, verifier=...)`
вызывает внешний `tests.e2e.audit.validate_freeze(path, expected_sha256)` после
серийной интеграции. Результат обязан связывать canonical spec SHA/source/inputs.
Harness не реализует второй алгоритм приёмки T18. Spec создаётся после source
commit вне исполняемого дерева; freeze/поздние отчёты не входят в хеш своего
исходного executable commit. Неизменяемые копии остаются в study.

`run_attempt(study, series, slot, adapter)` перед вызовом адаптера атомарно пишет
durable intent. Адаптер получает frozen spec и новый private attempt directory;
возвращает ровно `{outcome, observations, evidence_sha256}` и сохраняет исходные
логи/receipts. Outcome: passed/expected_rejection/failed/recovery_required. Ошибка
или незавершённый intent не разрешают повтор эффекта под тем же slot. Исправленная
серия требует новой явно объявленной ревизии; прежние failures не удаляются.

| Серия | Контракт observations, проверяемый аудитором |
|---|---|
| E1 | forced=true/run_cache=false/cache_hit=false/final_fit_events=1; fresh workspace/run/version; data/split/features/vocabulary/coordinates SHA; scores по group/candidate и metrics |
| E2 | measurement_id/pair/mode/order; human actor, attestation/journal SHA; одинаковый cache_state SHA и coordinates; HPO off; wall/active/setup/warmup/actions/errors |
| E3 | fault/repeat; own-resource isolation/fault receipt SHA; final consistency; injection/detection/restoration monotonic; упорядоченные HTTP `{t,ok}` samples |
| E4 | T17 receipt/policy checks/ACTIVE catalog; seed; actual deltas requests/runs/versions; same logical request после approved labels; два полных200/10 окна |

E2 импортируется через `ingest_human_measurement(..., attest=...)`, а не эффект
таймера агента. Проверка аттестации требует реального оператора и исходного журнала;
сами поля `actor_kind: human` не являются доказательством человечности. A3 разрешает
агенту demo approval, но не создавать человеческие измерения. Отсутствующие строки
остаются `not_run`/`incomplete`, без искусственных нулей или обещания ускорения.

Аудитор требует все ожидаемые slots и сохраняет каждый исход, сверяет fresh runs
с `registry_before`, запрещает cached replica и сравнивает ранги, features/vocabulary,
scores/metrics с `atol=1e-6, rtol=1e-5`. Равенство model binaries не требуется.
E3 показывает bounds простоя из HTTP samples отдельно от длительности операции;
если до/после failed samples нет healthy-наблюдений, upper bound неопределён и
trial не считается успешно измеренным. Это не второй release/recovery алгоритм.

## Однократная E5 и numeric-only восстановление

`HoldoutGuard` — native SQLite WAL/FULL ledger, общий для всех studies/candidates,
в доверенном Linux native volume. Реальный ledger не создаётся и не потребляется
до явного разрешения после freeze. API не предоставляет reset/force. Контур
рассчитан на доверенного локального оператора, не на защиту от администратора,
который может удалить том или подменить код.

Unique key — SHA canonical `{schema_version:1, root_policy_sha256,
protected_test_documents_sha256}`. Корневая policy закреплена; защищённые test
documents вычисляет T18 из hashed root metadata без чтения test body. Hash test
partition, candidate/protocol/model/bootstrap являются immutable **binding**, а
не новым ключом: namespace/serializer/candidate не разрешают повтор holdout.

`evaluate_once(study, guard, read_snapshot, score_both)` сначала COMMIT/FULL
фиксирует consumed_intent, затем ровно один раз вызывает loader общего snapshot
и общий scorer baseline+ML. Адаптеры повторно проверяют точный snapshot/artifact
binding; fit/HPO/warmup допускаются только на train/validation до этого шага.
Numeric raw содержит все scores/labels/groups, latency одного прохода по каждой
группе и artifact hashes; текстов в нём нет. Оба алгоритма оцениваются на одном
наборе пригодных групп существующим ranking evaluator.

Если numeric.json и commit.json уже полностью fsynced, `HoldoutGuard.complete`
может завершить ledger из точного результата без loader/scorer. Если их нет,
исход остаётся consumed/unknown: повторного test read нет. Старый failure receipt
не стирается при восстановлении. После completed receipt таблицы/графики строит
`rebuild` только из numeric raw; его checksum и frozen binding проверяются.

Парный bootstrap ресемплирует группы с возвращением2000раз с seed20261030 и
строит percentile95% для разницы model−baseline NDCG@10/Recall@10. При менее10
пригодных групп интервал остаётся null с причиной. Ранги и group eligibility
восстанавливаются из сохранённых чисел без model inference. CSV и JSON строятся
stdlib; `--figures` использует имеющийся Matplotlib, при его отсутствии завершает
ошибкой и ничего не устанавливает. Статистический выбор не меняется по test.

## Заполнение после разрешённых исследований

Каждое будущее число получит ссылку на frozen spec, immutable attempt/raw receipt,
исходные CSV/JSON и версию агрегирующего кода. В таблицах остаются отрицательные,
неудачные и неопределённые исходы с denominators. Synthetic/demo_only результаты
не доказывают пользу для реального найма или превосходство стека. До T18/разрешения
фактического исполнения все строки первой таблицы остаются `not_run`.
