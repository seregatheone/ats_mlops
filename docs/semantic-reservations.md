# Semantic fold reservations (#36)

Исторический splitter вычислял порядок компонент из ID. Генератор с тем же seed и новым namespace создавал те же тексты, но мог переносить frozen test в train/validation другого manifest. Метаданные T9 подтверждают такое пересечение; исторические модели 4/5 и отчёты остаются инженерными `demo_only` свидетельствами. Они не подтверждают независимое качество E5. Default API A4 и прежние артефакты этим исправлением не заменяются. Для финального E5 потребуется новый candidate с проверенной чистой lineage; обучение и E5 в scope этого исправления не входят.

Anchor — исходный `data_manifest.json` (schema 1), 60 компонент и прежние folds. Сохраняются SHA:

- train: `adef4de05783391cef598098b8886221bf4d8f28d9e296d74f94e8cf66a72ea7`;
- validation: `1cdd5f3189ba4241f5bb2399f33dac47f6b38e21a04819aa0c07900d1e29e0a8`;
- test: `35a6a0fc5c379c384ccd6ba645a4673f0f508df024d34f05355a40a1c9f14d65`.

Исходный manifest и файлы данных не мигрируются на месте. Ресурс `ats_mlops.data/reservations/canonical-v1.json` содержит fold и SHA256 каждого нормализованного word-токена для 1260 документов обоих типов. Текстов и меток в registry нет. Root policy SHA `4ca6c42155d3e34e1436596381838bf31cc36339986c12b91278d760df5d265b` закреплён в коде.

Builder использует только pinned код генератора и исходный manifest metadata, регенерирует строки в памяти и сверяет SHA всех трёх частей:

```sh
python scripts/build_reservation_policy.py
```

Он не открывает файлы частей, не обучает модель и не вычисляет test metrics. Identity генератора связывает точный source функции `generate_rows`, словарь навыков и версию, независимо от отличий `ast.dump` между Python 3.11/host. Whole-file SHA также записывается в новый manifest как provenance. Исторический whole-file SHA явно допущен для schema 1, поэтому изменение splitter не отзывает audit старых моделей.

Splitter сохраняет граф ID/точных/близких текстовых связей. Для каждого документа Jaccard вычисляется над хешированными token sets тем же порогом 0.9. Совпадение с anchor фиксирует fold всей компоненты. Связь с двумя защищёнными folds, ослабление порога или несовместимые квоты означают отказ до записи набора и до fit. Неизвестные компоненты упорядочиваются по seed и хешам содержимого, без ID/labels; независимая CI-синтетика с другим seed получает свои компоненты.

`prepare_dataset(..., reservation_policy=parent)` допускает явное расширение для новой версии данных: policy содержит parent, его SHA и новые хешированные документы. Предок неизменяем и должен завершаться trusted root; переопределение его folds запрещено. Для продолжения этой ветки передавайте её сохранённую policy, а не начинайте снова от root: разные ветки с независимыми неизвестными компонентами сами по себе не образуют общий registry. Скрытого глобального mutable state нет. Алгоритм обнаруживает token Jaccard duplicates и транзитивные связи; произвольные перефразирования или изменение большей части токенов не гарантированно узнаются.

Schema 2 manifest связывает `data/reservation_policy.json` и semantic assignments по SHA256. Bundle whitelist, Airflow ingress, prepared snapshot и DVC deps передают policy как metadata; DVC pointers для train/validation/split остаются прежними. Wheel включает root resource. GX admission дополнительно проверяет все новые части. T5/T8 `require_valid` проверяет train и validation вместе, включая связи между ними, прежде чем вернуть in-memory rows для fit. Schema 1, включая старый namespace-repartitioned manifest, отклоняется с `reservation_metadata_required` даже при прежнем passed GX receipt. Исторический audit не считается новым training admission.

Schema 2 создаётся в новом каталоге; canonical config воспроизводит прежние data bytes с новой metadata identity:

```sh
mkdir -p /tmp/new-canonical
PYTHONPATH=src python -m ats_mlops.cli data generate \
  --config configs/data.json --manifest /tmp/new-canonical/data_manifest.json \
  --output-dir /tmp/new-canonical/data
```

Повторный запуск разрешён только с тем же frozen test. Schema 1 на месте не перезаписывается. Восстановление исторических байтов из DVC и `data audit` остаются отдельными операциями с прежними hashes и явным чтением артефактов при полном аудите.

Focused proof на executable `ada48ea68c2b6a46b1b31350311f89b954ac92c5`: 36 host stdlib checks; под pinned Python 3.11.17 — 53 no-fit test methods с настоящим GX и двумя отказами конкретного training stage, затем 11 DVC/bundle/prepared checks после исправления harness tmpfs для `/var/tmp/dvc`. Builder подтвердил все три исходных SHA на обеих версиях Python. Wheel собран и установлен без сети; отдельный isolated process загрузил 1260 reservations из installed package вне checkout. Fit заблокирован в harness, вызовов 0, skips 0. Первый readonly-DVC setup failure сохранён как частичный прогон; он не выдаётся за полный PASS. Подробности: [data-reservations.json](../reports/data-reservations.json).

После интеграции T14 main `cd2c7d8` executable `732b4e8b6f13f9535c637bf5b11b165a791709d8` прошёл официальный полный `tests/unit`: 155 тестов за 55.268s под тем же cached Python 3.11 image, CPU1/memory1536MiB, networknone, checkout RO. Этот gate включает существующие tiny fixture fits и in-memory Optuna unit tests; отдельные pipeline/Registry training/registration не запускались. Host static83/http contract и transport3 PASS. Код reservation, policy resource и package-data не изменились, поэтому прежний focused/wheel proof переиспользуется явно. Настоящий remote Actions для интегрированного PR пока ожидается.
