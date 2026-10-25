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
