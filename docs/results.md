# Экспериментальные результаты T19

**E1, E3, E4 и E5 прошли; E2 остаётся `not_run`, общий статус — `incomplete`.**
Основная кампания `cwd2` выполнила три независимых обучения,18 опытов с
отказами, семь сценариев мониторинга и единственную итоговую оценку.
Пяти подлинных человеческих manual/auto пар нет. Агентские действия не
заменяют эти измерения, поэтому полный план E1–E5 не объявляется завершённым.

Источник таблиц — [публичный агрегат](../reports/experiments/t19/aggregates.json),
SHA256 `e5396e1bfece219ff5f569ebdf7c93eb3b906702da9640406205d3d661e86ee0`.
[Описание происхождения и пересборки](../reports/experiments/t19/README.md),
[сгенерированные таблицы](../reports/figures/t19/results.md) и
[manifest рисунков](../reports/figures/t19/manifest.json) сохраняют связь с его
байтами. Полный private backing требуется для проверки native происхождения;
публичный агрегат не является полным replayable freeze.

## Зафиксированная процедура

Предмет работы — **«Автоматизация жизненного цикла воспроизводимого машинного
обучения в интеллектуальных системах подбора персонала»**. Данные — синтетика
сопоставления навыков, `demo_only`; независимый реальный hiring corpus отсутствует.
[Протокол](experiment-protocol.md) и [контракт](data-contract.md) закреплены до
измерений. [T18](acceptance.md) описывает приёмку до формальных E1–E5 и
не переписывается задним числом.

| Связь | Значение |
|---|---|
| Исполняемый source | `14be8edffb68a2320404e8e1a03f37a740694466` |
| Training source FIRST S1 | `01ccf13d123857ea7b0c8c36fef6c29a13935b78` |
| Полный private freeze SHA256 | `3d081dba7f13ee46366b10417b1a8f7472da64bbe62220bb5c9de090807d6481` |
| Spec SHA256 | `74eff657e0974f1877af04645ca017aa2184c43afee58399c7aea7cec9d6049d` |
| Кандидат E5 | Первый обязательный S1 fit, private model1/run4be0804b65034beba65023b7f8572167 |

E5 использует этот заранее выбранный артефакт. Поздние E1/E4 версии,
включая обновлённый ACTIVE, его не заменяют. Номера версий относятся только
к своему private Registry. Параметры: schema2 v2, data seed20261027, model
seed20261026, C1/max_features2000, frozen selection, zero HPO, один numeric
thread; data/admission/reservation/split, locks, образы и ресурсы закрыты freeze.

## Учёт основной и остановленной кампаний

| Область | Завершённые fits | Учёт |
|---|---:|---|
| Основная `cwd2` | 4/4 | 3 E1 + 1 E4 |
| Сохранённая остановленная кампания | 3 | Вне основной оценки |
| Qualification T18 | 5 | 1 S1 + 3 S2 + 1 S6 |
| Всего записанных private fits | 12 | Изолированный CI исключён |

Первая кампания сохраняет15 успешных fault trials и один cwd/import failure
**до** crash checkpoint. Она остановлена; её результаты не прибавлены к
основным repetitions. Retained receipt SHA256:
`e936e6157bc8fe7ae619e6255115276c95206cb3857442bf3db0409d57cb5d85`.
Основная серия заново выполнена при том же freeze/source/FIRST S1 с явным
frozen source cwd. Неудачный прежний slot не переименован в успешный.

В private Registry после серий15 версий; version count включает zero-fit
fixtures и не равен числу обучений. Два отказа внешнего E4 observer до
grant/intent/slot также не считаются E4 attempts. Исправлена только внешняя
привязка к exact frozen environment, а не протокол или source.

## E1 — независимое воспроизведение

Три fresh forced fit создали3 новых runs/версии12–14. В каждом зафиксирован
`dvc repro -f --no-run-cache`, один final fit и отсутствие cache hit/run-cache.
Числовое сравнение выполнено при `atol=1e-6, rtol=1e-5`.

| Slot | Пар | Групп | Final fits | Max abs score diff | Max abs metric diff |
|---|---:|---:|---:|---:|---:|
| 1 | 220 | 11 | 1 | 0 | 0 |
| 2 | 220 | 11 | 1 | 0 | 0 |
| 3 | 220 | 11 | 1 | 0 | 0 |

Все replicas прошли допуск. Это результат заданных входов, seed, platform,
image и ресурсов; бинарное равенство pickle не является критерием.
[Рисунок E1](../reports/figures/t19/E1-reproducibility.svg).

## E2 — human manual/auto

**`not_run`:0/5 человеческих пар.** Нет проверенного журнала действий и
attestation реального оператора. Время агента, approval A3 и автоматические
CLI вызовы не считаются manual наблюдением. Медианы wall/active time,
actions/errors и ускорение не рассчитаны. Чередование порядка,
контроль кеша и отдельный setup/warmup остаются требованиями протокола.

## E3 — контролируемые отказы

Все18/18 attempts основной серии завершены, по3 на каждый вид. Registry
delta равен0, конечные API/catalog/alias согласованы. Три crash attempts
достигли durable checkpoint с реальным exit73 и прошли reconcile/rollback.

Ниже средние по трём попыткам, в секундах, округление до6 знаков после
запятой. Detection/restoration — длительности операции после инъекции;
две последние колонки — средние нижняя и верхняя границы HTTP-простоя,
а не доверительный интервал или точный downtime.

| Отказ | Attempts | Detection, s | Restoration, s | Outage lower, s | Outage upper, s |
|---|---:|---:|---:|---:|---:|
| `corrupt_data` | 3/3 | 7.036378 | 7.122795 | 0.000000 | 0.179673 |
| `minio_unavailable` | 3/3 | 6.987584 | 9.623256 | 0.000000 | 0.112389 |
| `incomplete_artifact` | 3/3 | 0.079751 | 0.166708 | 0.000000 | 0.106831 |
| `bad_candidate` | 3/3 | 4.033218 | 4.122584 | 0.000000 | 0.180506 |
| `unready_container` | 3/3 | 63.965900 | 64.057563 | 63.401278 | 63.614535 |
| `switch_alias_crash` | 3/3 | 3.090206 | 6.080795 | 4.935480 | 5.354942 |

Нижняя граница0 при сохранённых healthy probes не доказывает нулевой простой.
Редкие пробы ограничивают разрешение наблюдения. Продолжительный unready
startup отражён в измерениях, а не исключён ради удачного среднего.
Его средняя верхняя граница63.614535s и максимум63.766903s — около64s
в этом single local API. Восстановимость не означает zero downtime или
соблюдение production SLO.
Полные min/mean/median/max находятся в агрегате.
[Рисунок E3](../reports/figures/t19/E3-outage-bounds.svg) показывает средние
**верхние границы**, поэтому его нельзя читать как точное время простоя.

## E4 — drift, labels и повторный цикл

Пройдены7/7 заранее назначенных сценариев. Порог Wasserstein0.1/share0.5,
minimum200pairs/10vacancies, два окна, cooldown86400s и одна inflight заявка
не настраивались по результатам. Seed20261029 отделён от calibration1707.

| Сценарий | Новых окон | Пар / вакансий в каждом | Новых requests | Новых fits/runs | Новых версий |
|---|---:|---|---:|---:|---:|
| `control` | 2 | 220 / 11 | 0 | 0 | 0 |
| `false_signal` | 2 | 220 / 11 | 0 | 0 | 0 |
| `insufficient` | 2 | 20 / 1 | 0 | 0 | 0 |
| `shifted_no_labels` | 2 | 220 / 11 | 1 | 0 | 0 |
| `shifted_labeled` | 0 | нет новых окон | 0 | 1 | 1 |
| `replay` | 0 | нет новых окон | 0 | 0 | 0 |
| `cooldown` | 2 | 240 / 12 | 0 | 0 | 0 |

Shifted/no-labels создал одну логическую заявку без fit. Approved labels
возобновили **ту же** заявку и дали один fit/run/version. Replay/cooldown
не создали дубликатов. Нулевое число новых окон у labels/replay означает
продолжение уже существующего процесса, а не пустой измеренный набор.
После ACTIVE reference refresh полные cooldown окна содержали240 пар/
12 вакансий; исходные полные окна —220/11, insufficient —20/1.
[Рисунок E4](../reports/figures/t19/E4-effects.svg).

## E5 — единственная итоговая synthetic оценка

Завершена1/1 разрешённая попытка FIRST S1 против Jaccard baseline на одном
test snapshot:240 пар,12 групп, все12 пригодны. Итоговые таблицы построены
из retained numeric результата; повторного test read/scoring при публикации нет.

| Метрика | Jaccard baseline | FIRST S1 модель | Model−baseline | Percentile95% |
|---|---:|---:|---:|---|
| NDCG@10 | 1 | 1 | 0 | [0; 0] |
| Recall@10 | 1 | 1 | 0 | [0; 0] |

Bootstrap:2000 парных vacancy resamples, seed20261030, минимум10 пригодных
групп. Нулевая разность и интервал[0;0] отражают совпадение результатов на
этом synthetic наборе. **Выигрыш обучаемой модели по качеству не получен.**
Это не доказательство равенства методов на реальных данных или полезности
для найма. [Рисунок разности](../reports/figures/t19/E5-quality-difference.svg).

Latency — один измеренный проход на каждую из12 групп после прогрева только
на train/validation. Единицы следующей таблицы — **миллисекунды на группу**;
P95 описывает12 наблюдений, не нагрузочный benchmark API.

| Метод | Групп | Min, ms | Median, ms | P95, ms | Max, ms |
|---|---:|---:|---:|---:|---:|
| Jaccard baseline | 12 | 0.175166 | 0.181749 | 0.280008 | 0.381666 |
| FIRST S1 модель | 12 | 1.186291 | 1.352958 | 1.865864 | 1.917541 |

В этом проходе медиана model latency больше baseline. Это локальное измерение
двух scorers, без вывода об универсальном быстродействии.
[Рисунок latency](../reports/figures/t19/E5-latency.svg).

E5 numeric SHA256 `7c817d3cec5f9f3baafe454f092d2ada79271e4b681c21956e929968cd362e6d`;
commit SHA256 `9b282873403c1eaaeb14120f448c6d90175c5f1d0e3ab5c3dbd61a5a824752f0`;
completed receipt SHA256 `e0ad7eedb9555808c57311fb74a5c887c70a4a367ae82b27e052807e8dde1792`.
Ledger завершён; namespace/candidate changes не разрешают повтор holdout.

## Ограничения и состояние работы

Synthetic corpus не отражает разнообразие реальных резюме, решений
рекрутера и последствий для соискателей. Нет независимого hiring качества,
fairness результата или измерения человеческой экономии. Малые числа
repetitions и одна локальная platform ограничивают временные выводы;
конечные HTTP-пробы дают только outage bounds.

Public projection содержит агрегаты и хеши, без текстов, inventories
candidate/vacancy IDs, private paths и credentials. Полное происхождение
проверяется по сохранённому private графу; публичная пересборка лишь повторяет
таблицы/графики и не выполняет native эксперименты.

T20 clean-copy demo ещё не прошёл полный lifecycle. Первый demo-001 остановлен
до data container из-за stale image ID, закреплённого внешним recipe до
последнего bootstrap rebuild;0 fits, cleanup PASS, исход сохранён. Public
инструкция уже задаёт правильный порядок. Новый demo-002 с исправленным outside helper остановился на
pipeline bundle с CalledProcessError; причина исследуется,0 fits.
Source0c82 не менялся; полный demo остаётся неподтверждённым. Его будущие fits
не входят в formal T19. Общий статус остаётся **incomplete из-за E2**.
