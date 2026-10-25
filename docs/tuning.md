# T8: воспроизводимый поиск Optuna

`python -m ats_mlops.model.tune` выполняет последовательный seeded TPESampler
study с бюджетом **20 trials**, `n_jobs=1` и пространством из
`configs/search.json`. Оптимизируется validation NDCG@10 с тем же manifest
inventory и model-independent eligible cohort, что у baseline. Равные значения
выбираются по меньшему номеру trial. Все trial models создаются заново через
`fit_rows`; TF-IDF обучается только на train. Поиск не регистрирует модели.

Этап один раз вызывает fail-closed `require_valid` перед study и использует
возвращённые in-memory train/validation rows. Test/source/corpus/split-list bytes
не читаются. Report требует чистый опубликованный source SHA и pinned runtime.
Reports пишутся вне checkout в новый пустой каталог, study не перезаписывается.

```sh
python -m ats_mlops.model.tune \
  --manifest /work/data_manifest.json --report-dir /work/study \
  --admission-report /work/admission/data-admission.json \
  --config /source/configs/model.json --search /source/configs/search.json \
  --source-root /source --image-identity /work/image.json
```

`--smoke` задаёт отдельный эффективный бюджет **2**, сохраняя основной бюджет 20
в конфигурации и отчёте. Smoke не является выполненной основной серией. Timeout
можно ограничить через `timeout_seconds`; завершение до бюджета даёт failed
`incomplete_budget`, без selected config. Failed и undefined trials сохраняются
с санитарными error codes и не выбираются; отсутствие COMPLETE trials даёт
`no_complete_trials` и ненулевой CLI exit. Upstream exception text не сохраняется.

`study.json` содержит параметры/value/state/error каждой попытки, selected config,
seed/budgets, версии Optuna, data/split/inventory/config/metric/source/image/lock
hashes. `selected-model.json` создаётся только для успешного study. Финальное
обучение выполняется отдельно существующим T7 stage:

```sh
python -m ats_mlops.model.training \
  --manifest /work/data_manifest.json --report-dir /work/final-training \
  --admission-report /work/admission/data-admission.json \
  --config /work/study/selected-model.json --source-root /source \
  --image-identity /work/image.json --experiment ats-t8 \
  --registered-model ats-pair-ranking
```

Отдельный final-training receipt связывает immutable study hash с новым
run/model version и selected config hash. Study не ссылается обратно на receipt;
hash-cycle отсутствует. Регистрация не означает promote, alias или release.
Все свидетельства относятся к synthetic `demo_only`, а не к качеству найма.

Проверка: `python -m unittest discover -s tests/unit -p test_tuning.py`, затем
реальный двух-trial smoke в pinned ML image и отдельный final training.

## Actual evidence, 2026-10-26

Executable source был clean и опубликован до запуска:
`0c7f600e8f581d6cfc04260d5320e12f2581e5b9`. Последующий evidence commit не
заменяет этот SHA. Resolved image:
`sha256:574c48deb107cba3f0bdc982d3aa0ab1500de58ab1694052bb92efbcab13ec91`,
Python 3.11.17, Optuna 5.0.0. Новые зависимости и core services не создавались.

Focused unit: 11 PASS. Отдельный smoke выполнил 2/2 COMPLETE trials; основная
серия выполнила 20/20 COMPLETE trials с бюджетом 20. На неизменном validation
inventory baseline — 12 eligible groups и 240 пар — NDCG@10 = 1.0 у всех
попыток; это synthetic rule labels. Tie policy выбрала trial 0:
`classifier.C=1.0`, `features.max_features=2000`, seed модели остался 20261026.
Reserved test/source/corpus/split files удалены после admission, до обоих study
и финального training; они отсутствовали на протяжении этих этапов.

Финальное обучение заново вызвало T7 training с selected config основной серии,
создало run `42a8ece3ae394e0a8cc550f87ac9fff4` и точную Registry version
`models:/ats-pair-ranking/3`. Trials не создавали MLflow runs или модели.
В receipt проверены равенство selected config hash с final params hash и
равенство train/validation inventory hashes; study hash не изменялся.
Artifacts и reference fixture остаются в persistent MLflow и внешнем evidence.
В Git сохранены только текстовые отчёты без raw texts и private host paths:

- [20-trial study](../reports/studies/study.json), [2-trial smoke](../reports/studies/smoke.json);
- [selected config](../reports/studies/selected-model.json);
- [final training manifest](../reports/studies/final-training.json),
  [one-way receipt](../reports/studies/final-training-receipt.json);
- [commands/checks](../reports/studies/checks.json),
  [training guard](../reports/studies/data-training.json),
  [admission](../reports/studies/data-admission.json).
