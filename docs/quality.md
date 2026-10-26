# T11: проверка качества кандидата

Standalone CLI создаёт новый `gate_report.json`, не выполняя fit, подбор,
регистрацию, согласование или активацию. Candidate, Jaccard baseline и наличный
incumbent получают одни полные validation rows и один набор eligible групп из
общего evaluator T6. Модельные scores не берутся из старых evaluation reports.

Рабочий предел снижения NDCG@10 — **0.005** отдельно относительно baseline и
incumbent; граница включена. Сравнение десятичных значений не добавляет скрытый
epsilon. Recall@10 публикуется без нового порога. Нулевой eligible cohort,
неконечные/неполные scores или нарушение lineage дают `outcome=rejected`.

`outcome=passed|rejected` отделён от `evidence_scope`. Scope берётся из допущенной
текущей validation, его нельзя повысить параметром CLI. В этом проекте допущен
синтетический `demo_only`; loader T3 не выдаёт его за independently labeled
корпус. Чистая policy поддерживает оба значения scope и отвергает плохие входы
при любом из них. Passed model gate ещё требует будущих release checks и approval.

## Входы и provenance

Host coordinator использует стандартную библиотеку Python и Docker CLI.
`configs/quality.yaml` записан в JSON subset YAML, чтобы не устанавливать YAML
зависимость в host interpreter. Сохраняются raw file hash и canonical threshold
hash. Предпочтителен Python 3.11; actual host version записывается в report.

Пример request; пути заменяются на собственные подготовленные артефакты:

```json
{
  "validation_manifest": "/work/workspace/artifacts/prepared/data_manifest.json",
  "admission_report": "/work/workspace/artifacts/prepared/admission.json",
  "dataset_binding": "/work/workspace/dataset-binding.json",
  "thresholds": "/source/configs/quality.yaml",
  "candidate": {
    "run_manifest": "/work/candidate/run_manifest.json",
    "pipeline_manifest": "/work/candidate/pipeline_manifest.json",
    "model_dir": "/work/candidate/model"
  },
  "incumbent": null
}
```

```sh
PYTHONPATH=src python -m ats_mlops.quality.gate \
  --request .local/gate-request.json --output .local/quality/gate-001.json
```

Для incumbent передаётся такой же descriptor с обязательным `release_id`.
`release_is_fixture=true` явно отмечает инженерную фикстуру; она не считается
существующим опубликованным выпуском. Candidate/incumbent именуются точной
числовой Registry version, run/model/pipeline-manifest hashes. Alias/latest
отвергаются. Новый непустой report не перезаписывается. CLI возвращает 0 только
при passed; rejected/request error дают 1.

Candidate обязан соответствовать текущему manifest, admission, train/validation
inventories и split. Проверяются authoritative T9
`inputs.binding.workspace_pointers` и фактические current `*.dvc` bytes/MD5/size;
для этого `dataset_binding` лежит в корне исходного workspace. Старые pointers в
run manifest/source snapshot не подменяют эти данные. Gate не читает train,
source, corpus, split-list или test contents: нужны validation bytes, admission
и metadata. Incumbent может быть обучен на прежних данных; его собственная
полная lineage проверяется относительно его sidecar, затем он заново оценивается
на текущей validation. Его старый data manifest не обязан равняться текущему.

Локальные model directories берутся из согласованных T9 outputs либо скачиваются
отдельно по exact Registry URI. Sidecar/manifest должны относиться к той же
версии: directory другого run не становится допустимым при совпадении названия.
Gate проверяет полный model/payload inventory, точный bundled source inventory,
source/config/parameter hashes и встроенный input provenance до исполнения кода.
Symlink, `.pyc`, `.pyo` и даже пустой `__pycache__` запрещены; старый inventory
helper сам по себе эти файлы скрывает. Sidecars и артефакты должны происходить из
доверенного версионированного pipeline: pickle исполняет доставленный код.

## Отдельный runtime на каждую модель

Каждый scorer — новый контейнер с локальным exact image ID из lineage, без pull,
сети, checkout mount, Docker socket или переданных credentials. Mounts содержат
только read-only artifact и приватный validation input; root filesystem read-only,
temporary storage ограничено. Лимит — один CPU/1.5 GiB, BLAS threads=1.

`python -I -B` сначала проверяет отсутствие `ats_mlops` в import path/sys.modules,
runtime Python/packages/lock и полный inventory. Перед unpickle проверяются
MLmodel flavor/run/version/feature metadata. После native load используются
bundled scoring/feature code, проверяются module paths, fitted feature order и
effective classifier params. Candidate и incumbent загружаются в разных
процессах, поэтому одинаковые имена модулей не смешивают их код.

Raw validation texts существуют только в приватном временном input. Отчёт
содержит IDs, hashes, числовые метрики/разности, безопасные причины и scoring
receipts; texts/credentials и upstream exception messages не публикуются.
Временные контейнеры удаляются; timeout удаляет только контейнер данного scorer.

Expected incumbent model/release IDs фиксируются в report. T13 должен повторно
сверить их под activation lock: этот model gate не разрешает использовать stale
incumbent, переключать release или обходить последующие проверки.

Проверка: `python -m unittest discover -s tests/unit -p test_quality_gate.py`.
Структурные unit fixtures не исполняют pickle; реальные isolated artifact checks
выполняются отдельно на закреплённых model versions без обучения.
