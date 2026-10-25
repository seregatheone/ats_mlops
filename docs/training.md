# T7: обучение и переносимая модель

`python -m ats_mlops.model.training` обучает один sklearn Pipeline: общий TF-IDF
двух train text columns → cosine/Jaccard/log token lengths/length ratio →
LogisticRegression. Validation только преобразуется. Нормализация и порядок пяти
признаков версионированы в `features/pairs.py`, параметры — `configs/model.json`.
`make_pipeline`/`fit_rows`, `pair_frame`, `positive_scores` и `score_rows` являются
общим API для будущих Optuna, gates и inference. Labels и IDs не входят в признаки.

Этап требует существующий GX admission receipt. Сначала вызывается
`require_valid(manifest, reports, admission_report=...)`; fit получает возвращённые
in-memory rows. Последующее чтение train/validation не требуется. Test, source,
corpus и split-list bytes в регулярном этапе не читаются. Пустой словарь и один
класс отклоняются до регистрации модели; некорректные probabilities запрещены.

Исполняемый source сначала коммитится и отправляется в task branch. Обучение
требует clean checkout этого SHA и lockfile, совпадающий с lock runtime image.
Reports создаются вне checkout. Передайте JSON `--image-identity` с resolved image
`id`, reference, repo_digests, os и architecture, полученными `docker image inspect`.
Пример запуска внутри подготовленного cached ML image, с readonly checkout:

```sh
python -m ats_mlops.model.training \
  --manifest /work/data_manifest.json --report-dir /work/training \
  --admission-report /work/admission/data-admission.json \
  --config /source/configs/model.json --source-root /source \
  --image-identity /work/image.json --experiment ats-t7 \
  --registered-model ats-pair-ranking
```

MLflow tracking задаётся `MLFLOW_TRACKING_URI`. Default core проксирует artifacts;
клиенту не нужны S3/SQL credentials. Pipeline логируется с обоими flavors,
`code_paths=["src/ats_mlops"]`, cloudpickle и pyfunc `predict_proba`. Код попадает в
artifact `code/ats_mlops`; зависимости задаются pinned requirements, а runtime
работает из существующего locked image. Loader не устанавливает зависимости.
Сохраняется точная числовая `models:/name/version`, source/run binding проверяется
через Registry client. Alias не используется для свидетельства или serving.

`run_manifest.json` связывает реальный clean Git SHA, source hashes, DVC pointers,
проверенные train/validation data hashes, admitted split/inventory hashes,
feature/params/seed, package/platform/lock/image identities, run/version и hashes
модели/уже существующих reports. Содержимое model directory хешируется отдельным
canonical inventory; `model.pkl` имеет отдельный payload hash. Manifest хранится
вне model directory и не хеширует сам себя. Будущий portability report ссылается на
замороженный manifest; его hash не добавляется обратно в manifest. Evidence commit
может быть позднее training SHA и не заменяет его.

Проверки:

```sh
python -m unittest discover -s tests/unit -p test_training.py
ATS_RUN_MLFLOW_LINEAGE=1 ATS_T7_EVIDENCE_DIR=/absolute/external/evidence \
  python -m unittest discover -s tests/integration -p test_mlflow_lineage.py
```

Integration запускается на Docker host, использует уже запущенный default core
(`ats-core_default`, MLflow `http://mlflow:5000`) и cached `ats-core-ml:t4`. Она не
строит и не перезапускает core. Создаёт отдельный чистый clone, детерминированно
генерирует admitted корпус, сравнивает manifest с замороженным и удаляет reserved
файлы до training. Сохраняет actual run/model version и получает model artifact
через exact Registry URI. Затем два свежих контейнера с `--network none`, readonly
artifact/fixture и без checkout/credentials выполняют `python -I` при
`PYTHONPATH=''`: native/pyfunc probabilities и class-1 scores должны совпасть
(`rtol=0`, `atol=1e-12`). Native vocabulary/IDF и source hashes проверяются отдельно;
удаление упакованного кода должно сломать fresh load. Skip не является PASS.

Core image содержит старый `/opt/project/src`: очистить PYTHONPATH недостаточно,
поэтому проверяются isolated interpreter и отсутствие `ats_mlops` до load.
Предварительный import проекта может загрязнить `sys.modules`. Будущий Bento
release должен загружать exact artifact первым в отдельном контейнере и проверять
feature code/version binding. BentoML 1.4.40 `mlflow.import_model` переносит полный
MLflow directory; его `load_model` использует pyfunc. Это сохраняет выбранный
контракт probability matrix без повторной сериализации sklearn.

Свидетельства относятся к synthetic `demo_only` validation. Они подтверждают
инженерную переносимость на зафиксированной платформе, не качество реального
найма и не прохождение будущего release gate.
