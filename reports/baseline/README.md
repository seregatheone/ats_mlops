# Baseline validation T-6

26.10.2026. [validation.json](validation.json) — фактическая инженерная оценка
`jaccard-v1` на замороженном validation demo-v1: 240 пар, 12 полных групп,
12 пригодных, 0 без положительных меток, 0 пустых. NDCG@10 = 1.0,
Recall@10 = 1.0. Scope — **demo_only**: метки созданы синтетическим правилом
сопоставления навыков, поэтому эти значения не доказывают качество реального
найма, независимую валидность меток или готовность выпуска.

Baseline не обучается. Он использует общий `data.dataset.text_tokens`: NFKC,
сжатие пробелов, Unicode casefold и множество Unicode word tokens. Score —
мощность пересечения токенов / мощность объединения; пустое объединение даёт
0. Полные группы сортируются по score убывающе, затем `candidate_id`
возрастающе. Метки не используются в scoring.

Общий evaluator `ats_mlops.evaluation.evaluate_ranking(rows, scores,
group_ids=None)` принимает scores по ключам `(vacancy_id, candidate_id)`.
Их множество обязано точно совпадать со всеми размеченными rows; повтор пары,
небинарная метка, пустой ID, нечисловой/бесконечный score и NaN отклоняются.
`group_ids` — опциональная полная inventory, позволяющая представить пустые
группы; дубликаты и группы rows вне inventory отклоняются. Полноту rows
относительно внешнего корпуса подтверждает loader, а не восстановление
отсутствующих кандидатов или меток. Candidate/incumbent должны получать
одинаковые полные rows и inventory; отчёт сохраняет `eligible_group_ids`.

Для каждой группы k = min(10, n). DCG использует бинарный gain и discount
`1/log2(rank+1)` для one-based rank. IDCG ставит `min(k, positive_count)`
единиц первыми. Recall делит число положительных в top-k на число всех
положительных группы. Обе macro-метрики усредняются по одному набору групп
с хотя бы одной положительной меткой. `zero_positive_group_count` включает
пустые группы; `empty_group_count` отдельно показывает это подмножество.
Доля zero-positive делится на число всех групп (0.0 при пустой inventory).
При отсутствии пригодных групп `status=undefined`, обе метрики JSON `null`;
CLI сохраняет такой отчёт и возвращает 2. Это не успешный quality gate.

Ручной тест: 11 кандидатов, положительные на местах 2, 4 и 11. Тогда
NDCG@10 = `(1/log2(3)+1/log2(5))/(1+1/log2(3)+1/2)`, Recall@10 = `2/3`.
Тест проверяет точное совпадение, границу k, общий macro cohort,
неопределённые/пустые группы, ничьи и инвариантность к перестановкам.

## Повторение

Из корня checkout с Python 3.11 и восстановленными данными T-3:

```sh
PYTHONPATH=src python -m ats_mlops.evaluation.baseline \
  --manifest data_manifest.json --output reports/baseline/validation.json
PYTHONPATH=src python -m unittest discover -s tests/unit -p test_ranking_metrics.py
```

В чистом checkout сначала восстановите детерминированные данные по
[карточке T-3](../../docs/data-card.md). Baseline CLI проверяет manifest,
ссылку на допущенный SHA256 split metadata и только train/validation bytes/inventory/classes,
затем оценивает validation. Он не вызывает полный audit и не открывает
source/corpus/test/split. Train нужен только для проверки допущенного входа,
никаких fit или настройки на validation нет. В сохранённом отчёте есть
SHA256 manifest, ссылочный SHA256 split (уже проверенный при admission T-3),
канонических inventory только train/validation, train/validation
и исходников, а также версия Python. Исходники идентифицируются содержимым,
поэтому отчёт не требует самоссылочного Git commit hash. Изменяется только
наблюдаемое `elapsed_seconds`; scores, cohort и метрики детерминированы.

Проверка выполнена в Python 3.11.17 из кешированного T-2 образа
`ats-compat-ml-base:t2`, image ID
`sha256:d51880bb52f2178c9434fd0b92a32052a1e935865a006072b5f1c146a2ab0597`.
Все команды выполнялись с Docker `--network none`, без новых зависимостей.
Перед actual baseline CLI файл `data/test.jsonl` был удалён. Unit guard
дополнительно запрещает открытия test/source/corpus/split при формировании отчёта.
После восстановления manifest остался побайтно идентичным tracked версии.
Focused suite: 14 PASS; полный unit suite: 32 PASS. После уточнения границы
metadata IO повторены 4 report-теста, остальные проверки не менялись. Negative fixture CLI
с повреждённым validation даёт exit 1 до scoring; undefined даёт 2.
Временные fixture names и elapsed time не являются исследовательскими данными.
