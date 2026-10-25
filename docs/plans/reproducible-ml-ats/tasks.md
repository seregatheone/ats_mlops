# Tasks: реализация диплома по автоматизации жизненного цикла ML в ATS

> Plan: ./plan.md. Внешней спецификации пока нет; требования R-1–R-8 определены в плане. Все пути ниже относительны `ats_mlops/`, если явно не указано иное. Это будущие задачи и проверки: команды и тесты ещё предстоит создать. Оценка задачи — рабочие дни одного разработчика без ожидания внешних данных; задачи могут выполняться параллельно при независимых входах.

## T-1 — Зафиксировать прикладную постановку и протокол
- after: none
- files: `docs/requirements.md`, `docs/data-contract.md`, `docs/experiment-protocol.md`, `docs/decisions.md`
- estimate: 2–3 дня
- acceptance: GIVEN тема и материалы курсовой WHEN постановка зафиксирована THEN указаны границы ранжирования, R-1–R-8, схема меток, основной/синтетический сценарии, правила split, критерии воспроизводимости, E-1–E-5, оборудование и решения о новых зависимостях. Источники DOCX и расширенных заметок различены. До добавления scikit-learn/служебной БД получено согласование; при отказе зависимые задачи перепланированы.
- check: проверка полноты по таблице R-1–R-8 и E-1–E-5, запись выбранного источника данных/допустимого синтетического варианта и согласованных решений в `docs/decisions.md`; отсутствие незакрытых входных решений для T-2/T-3.
- satisfies: R-1, R-8

## T-2 — Создать каркас и проверить совместимость стека
- after: T-1
- files: `pyproject.toml`, `requirements*.lock`, `src/ats_mlops/cli/`, `docker/`, `.gitignore`, `.env.example`, `docs/versions.md`, `tests/integration/test_compatibility.py`
- estimate: 2–3 дня
- acceptance: GIVEN согласованные зависимости WHEN выполняется короткий проход на синтетической модели THEN модель сохраняется в MLflow, импортируется в BentoML и отвечает из отдельного контейнера; DVC читает/пишет тестовый объект MinIO, GX выполняет validation. Задача LocalExecutor реально вызывает ML CLI через `/opt/ml-venv/bin/python`, в отдельном checkout указанного SHA, и пишет manifest с SHA/lock hash. Несовпадение lockfile checkout и образа отклоняется. Версии и платформа зафиксированы, приватных данных/секретов в Git нет.
- check: `python -m compileall src`; `python -m unittest discover -s tests/integration -p test_compatibility.py`; проверка `docs/versions.md` и lockfiles. Временные ресурсы compatibility-прохода удалены, результаты smoke записаны.
- satisfies: R-1, R-4, R-7

## T-3 — Подготовить данные, разметку и замороженное разбиение
- after: T-1, T-2
- files: `src/ats_mlops/data/`, `configs/data.json`, `data/*.dvc`, `data_manifest.json`, `tests/fixtures/`, `tests/unit/test_split.py`, `docs/data-card.md`
- estimate: 3–5 дней; поиск/ручная разметка могут потребовать дополнительного времени
- acceptance: GIVEN допущенный корпус WHEN выполняются очистка и split THEN manifest содержит происхождение, условия использования, размеры и хеши; вакансии/кандидаты и дубликаты не пересекаются между частями; классы и размеры групп позволяют вычислить выбранные метрики. Непригодный корпус отклоняется, для CI подготовлена отдельная синтетика. Test заморожен и исключён из поиска параметров.
- check: `python -m unittest discover -s tests/unit -p test_split.py`; `python -m ats_mlops.cli data audit --manifest data_manifest.json`; отчёт counts/duplicates/intersections и проверка хеша test.
- satisfies: R-1, R-2, R-8

## T-4 — Развернуть постоянные MinIO и MLflow
- after: T-2
- files: `compose.yaml`, `docker/`, `configs/storage/`, `docs/runbook.md`, `tests/integration/test_storage_tracking.py`
- estimate: 2–3 дня
- acceptance: GIVEN профиль core WHEN стенд стартует, сохраняет датасет/run/model version и перезапускается THEN артефакты и metadata сохранены. DVC и MLflow используют раздельные области/права, registry имеет SQL backend, служебные БД не открыты на внешнем интерфейсе. Ошибка credentials не возвращает ложный успех.
- check: `docker compose config --quiet`; `python -m unittest discover -s tests/integration -p test_storage_tracking.py`; ручной restart core, чтение сохранённых объектов и замер ресурсов.
- satisfies: R-1, R-3

## T-5 — Внедрить проверки качества данных
- after: T-3, T-4
- files: `great_expectations/`, `src/ats_mlops/data/validate.py`, `tests/unit/test_data_validation.py`, `tests/fixtures/invalid/`
- estimate: 2 дня
- acceptance: GIVEN валидные и испорченные фикстуры WHEN выполняется validation THEN отсутствующие поля, пустые тексты, повтор пары, недопустимая relevance и пересечение split отклоняются с конкретной причиной. Машинный и читаемый отчёты сохранены; при критической ошибке train не вызывается.
- check: `python -m unittest discover -s tests/unit -p test_data_validation.py`; проверка кода завершения CLI и отсутствия train-артефакта после негативного входа.
- satisfies: R-2

## T-6 — Реализовать baseline и единый расчёт метрик
- after: T-3, T-2
- files: `src/ats_mlops/model/baseline.py`, `src/ats_mlops/evaluation/`, `tests/unit/test_ranking_metrics.py`, `reports/baseline/`
- estimate: 2 дня
- acceptance: GIVEN размеченная группа WHEN baseline и evaluator получают её THEN возвращаются детерминированный порядок, NDCG@10/Recall@10, число оценённых/неоценённых групп. Ручной пример совпадает с расчётом; группы без положительных меток исключены из macro-average обеих метрик и посчитаны отдельно, пустые группы/ничьи обработаны по плану. Нулевое число пригодных групп даёт undefined, а не успешную оценку. На этом этапе используются train/validation; test не раскрывается.
- check: `python -m unittest discover -s tests/unit -p test_ranking_metrics.py`; сохранённый baseline validation report со split hash.
- satisfies: R-4, R-8

## T-7 — Обучить простую модель и зарегистрировать происхождение
- after: T-4, T-5, T-6
- files: `src/ats_mlops/{features,model,lineage}/`, `configs/model.json`, `tests/unit/test_training.py`, `tests/integration/test_mlflow_lineage.py`
- estimate: 3 дня
- acceptance: GIVEN train/validation и фиксированные параметры WHEN модель обучается THEN vectorizer использует только train, преобразования входят в модель, MLflow хранит параметры/метрики/артефакты и manifest связывает Git/DVC/split/environment. Отдельный процесс без training checkout загружает модель и получает совпадающие scores; пустой словарь или вырожденный класс дают понятную ошибку обучения.
- check: `python -m unittest discover -s tests/unit -p test_training.py`; `python -m unittest discover -s tests/integration -p test_mlflow_lineage.py`; тест с недоступными test-файлами.
- satisfies: R-1, R-3, R-4

## T-8 — Добавить ограниченный поиск Optuna
- after: T-7
- files: `src/ats_mlops/model/tune.py`, `configs/search.json`, `tests/unit/test_tuning.py`, `reports/studies/`
- estimate: 1–2 дня
- acceptance: GIVEN фиксированный sampler seed и пространство WHEN выполнен последовательный study с бюджетом 20 trials THEN сохранены все попытки, ошибки trials и выбранные параметры; оптимизируется validation NDCG, test не читается. Финальная модель отдельно обучается с выбранной конфигурацией.
- check: `python -m unittest discover -s tests/unit -p test_tuning.py`; smoke с двумя trials; бюджет 20 проверяется в конфигурации основной серии, конфигурация и study сохранены.
- satisfies: R-1, R-3

## T-9 — Описать воспроизводимый DVC DAG
- after: T-5, T-7, T-8
- files: `dvc.yaml`, `dvc.lock`, `params.yaml`, `src/ats_mlops/cli/`, `tests/integration/test_recompute.py`, `docs/reproducibility.md`
- estimate: 2–3 дня
- acceptance: GIVEN зафиксированные входы WHEN DAG запускается локально THEN validate/prepare/train/evaluate выполняются в нужном порядке и сохраняют manifest. Изменение входа инвалидирует нужные стадии, повтор без изменений отмечается cache_hit. В двух новых каталогах принудительный режим создаёт новые training runs и совпадающие результаты по допуску плана.
- check: `dvc dag`; `dvc status`; `python -m unittest discover -s tests/integration -p test_recompute.py`; сохранённые команды `dvc repro -f --no-run-cache` и manifest каждого пересчёта. Третий независимый пересчёт относится к E-1.
- satisfies: R-1, R-2, R-3

## T-10 — Оркестрировать запуск и повторы в Airflow
- after: T-9, T-4
- files: `dags/train_candidate.py`, `src/ats_mlops/cli/runner.py`, `compose.yaml`, `tests/integration/test_airflow_run.py`
- estimate: 2–3 дня
- acceptance: GIVEN dataset version и Git SHA WHEN DAG запущен THEN DVC работает в отдельной копии этого SHA, XCom содержит только ID/URI, max_active_runs=1. Временный сбой I/O повторяется максимум два раза после исходной попытки, дефект данных завершается без повторного обучения, cache_hit не выдаётся за новый MLflow run. Повтор завершённого шага не создаёт дубликат выпуска.
- check: импорт DAG без ошибок; `python -m unittest discover -s tests/integration -p test_airflow_run.py`; ручной запуск DAG и один управляемый I/O-сбой с проверкой журнала попыток.
- satisfies: R-2, R-3

## T-11 — Реализовать отчёт проверки кандидата
- after: T-6, T-7, T-9
- files: `src/ats_mlops/quality/`, `configs/quality.yaml`, `tests/unit/test_quality_gate.py`
- estimate: 2 дня
- acceptance: GIVEN candidate, baseline и incumbent WHEN gate оценивает их на одном validation manifest THEN недостающая lineage, NaN/inf или снижение NDCG более 0,005 приводят к rejected с причиной; отсутствие incumbent обрабатывается как первый выпуск. Отчёт имеет точные model version/split hash, incumbent model/release ID, hash порогов, отдельные outcome и evidence_scope. passed+demo_only допускает инженерный выпуск, rejected блокирует любой scope, отсутствие пригодных меток не означает passed. Test не используется.
- check: `python -m unittest discover -s tests/unit -p test_quality_gate.py`; фикстуры первого выпуска, хорошего/плохого кандидата, подменённого split и неполного manifest.
- satisfies: R-1, R-5

## T-12 — Упаковать модель в BentoML и реализовать API
- after: T-7, T-11
- files: `service/`, `docker/serving/`, `src/ats_mlops/cli/package.py`, `tests/integration/test_rank_api.py`
- estimate: 2–3 дня
- acceptance: GIVEN точный проверенный MLflow model version WHEN Bento запускается в новом контейнере THEN rank/metadata выполняют контракт плана, модель и preprocessing не зависят от training checkout, смена registry alias не меняет scores. Release manifest заморожен до проверки и не содержит будущего отчёта; `release_check_report.json` содержит model version/hash, image ID и неизменённый release manifest hash. Staging пишет events в отдельный временный журнал. Ограничения размера/ID/top_k, пустой список и 503 проверены; полный prediction-event сохранён до top_k и ответа, идемпотентный retry работает после рестарта. В технические логи не попадают тексты/контакты.
- check: `python -m unittest discover -s tests/integration -p test_rank_api.py`; контрольный запрос к контейнеру без доступа к каталогу обучения; проверка логов на маркер секретного текста фикстуры.
- satisfies: R-1, R-4

## T-13 — Реализовать согласование, переключение и откат
- after: T-12, T-11
- files: `src/ats_mlops/release/`, `configs/releases/`, `tests/integration/test_release.py`, `docs/runbook.md`
- estimate: 3–4 дня
- acceptance: GIVEN packaged release WHEN выполнены stage, approve и activate THEN оба отчёта имеют outcome=passed и привязаны к этим model version/image ID; receipt ссылается на неизменяемые хеши manifest/отчётов. passed+demo_only разрешает синтетическую демонстрацию с явным scope. Под блокировкой approve/activate проверяют совпадение incumbent gate с текущим выпуском; C со старым gate против A после выпуска B отклоняется до нового gate/ревизии/проверки/согласования. Журнал отражает переходы, API отвечает версией manifest, champion обновляется после готовности. Подмена артефакта, rejected при любом scope, отсутствие согласования и параллельная команда отклоняются. Сбой API восстанавливает предыдущий образ. После прерывания reconcile восстанавливает согласованность либо блокирует следующий выпуск с явной причиной. Сохранены предыдущие два выпуска.
- check: `python -m unittest discover -s tests/integration -p test_release.py`, включая неизменность хешей после approve, A→B→устаревший C и passed/demo_only против rejected; ручная демонстрация первого выпуска, обновления, отказа, rollback и reconcile с фиксацией времени перерыва.
- satisfies: R-5

## T-14 — Настроить GitHub Actions для изолированной проверки
- after: T-2, T-9, T-12, T-13
- files: `.github/workflows/ci.yml`, `docker/ci/`, `tests/fixtures/`, `docs/ci.md`
- estimate: 2 дня
- acceptance: GIVEN PR или push WHEN запускается CI THEN сборка/статические/unit/API/интеграционные проверки выполняются на синтетике с временными MinIO/MLflow, без локальных secrets и адресов стенда. Неправильный контракт проваливает job. Публикация реальных данных и переключение локального сервиса в CI отсутствуют.
- check: `docker compose -f docker/ci/compose.yaml config --quiet`; те же команды локально; успешный Actions run и временно внесённая/затем убранная негативная фикстура, вызвавшая ожидаемый fail. Доступ к GitHub нужен только на этом этапе; локальная проверка не заменяет доказательство состоявшегося CI.
- satisfies: R-7

## T-15 — Подключить Prometheus и Grafana
- after: T-10, T-12, T-13
- files: `monitoring/`, `src/ats_mlops/monitoring/technical.py`, `tests/integration/test_metrics.py`
- estimate: 1–2 дня
- acceptance: GIVEN rank-запросы и исходы DAG WHEN Prometheus опрашивает стенд THEN наблюдаются requests/errors/latency и состояние pipeline/release; Grafana показывает эти данные. У request_id/candidate_id и текста нет Prometheus labels; после рестарта конфигурация панели сохраняется.
- check: `python -m unittest discover -s tests/integration -p test_metrics.py`; запросы к Prometheus API, сверка счётчиков и ручной просмотр панели под нагрузкой.
- satisfies: R-6

## T-16 — Добавить Evidently и учёт независимой обратной связи
- after: T-3, T-7, T-12, T-15
- files: `src/ats_mlops/monitoring/{drift,feedback,quality}.py`, `configs/monitoring.yaml`, `tests/unit/test_monitoring.py`, `reports/monitoring/`
- estimate: 2–3 дня
- acceptance: GIVEN reference manifest, полные prediction-events и частичные метки WHEN отчёт построен THEN показаны числовой drift, качество только по полностью размеченным группам и покрытие по парам/группам. Запрос top_k=3 из 100 сохраняет все 100 scores; запоздалые метки присоединяются к исходной версии, дубли не меняют результат, конфликтующий ID отвергается. Политика label_revision/cutoff проверена. Отсутствие меток не превращается в нулевое качество; test не используется как reference. На калибровочной части закреплены drift tests/пороги, контрольное и искусственно изменённое окна различаются по заданному сценарию.
- check: `python -m unittest discover -s tests/unit -p test_monitoring.py`; JSON/HTML отчёты с версиями, сохранённые настройки калибровки и тест неполной/запаздывающей разметки.
- satisfies: R-6, R-8

## T-17 — Замкнуть цикл переобучения
- after: T-10, T-11, T-13, T-16
- files: `dags/monitor_and_retrain.py`, `src/ats_mlops/monitoring/policy.py`, `tests/integration/test_retraining.py`
- estimate: 2–3 дня
- acceptance: GIVEN два сигнальных окна допустимого размера WHEN policy вызывается THEN появляется одна заявка, повтор в cooldown не дублирует её. Без новой одобренной разметки состояние waiting_for_labels; с dataset v2 создаётся кандидат через обычный конвейер. Test не попадает в обучение, прогнозы не считаются истинными метками; плохой кандидат сохраняет активную модель, хороший ожидает штатного согласования.
- check: `python -m unittest discover -s tests/integration -p test_retraining.py` с управляемыми часами; ручная демонстрация drift → waiting → dataset v2 → candidate → gate → согласованный выпуск.
- satisfies: R-2, R-3, R-5, R-6

## T-18 — Провести приёмку и заморозить условия экспериментов
- after: T-13, T-14, T-15, T-16, T-17
- files: `tests/e2e/`, `docs/experiment-protocol.md`, `docs/acceptance.md`, `reports/acceptance/`
- estimate: 2–3 дня
- acceptance: GIVEN готовый стенд WHEN выполнены восемь итоговых сценариев плана THEN каждый имеет отчёт/лог с входными версиями и итогом; неполный артефакт, I/O-сбой, плохой кандидат, crash переключения и повтор триггера воспроизведены. После исправлений нет открытых дефектов, препятствующих R-1–R-8. Протокол E-1–E-5, версии, thresholds и ресурсы заморожены до измерений.
- check: `python -m unittest discover -s tests/e2e`; ручной L5 по `docs/acceptance.md`; сверка матрицы R-1–R-8 → тест/свидетельство, отдельная запись о доступности независимого корпуса.
- satisfies: R-1, R-2, R-3, R-4, R-5, R-6, R-7, R-8

## T-19 — Выполнить экспериментальную серию
- after: T-18
- files: `src/ats_mlops/evaluation/experiment.py`, `reports/experiments/`, `docs/results.md`
- estimate: 3–5 дней
- acceptance: GIVEN замороженный протокол WHEN выполнены E-1–E-5 THEN сохранены исходные CSV/JSON и manifests: три принудительных пересчёта, пять paired manual/auto прогонов, минимум три повтора каждого отказа, контроль/возмущения мониторинга и одна финальная test-оценка baseline/замороженной модели. Нет повторной настройки по test. Для отсутствующих независимых меток границы выводов соответствуют заранее выбранному синтетическому сценарию.
- check: отчёт аудита числа прогонов/версий/пустых результатов; скрипт строит таблицы/графики повторно из исходных файлов; разница scores и ранжирования проверена автоматически. Неудачные и отрицательные результаты сохранены.
- satisfies: R-1, R-8

## T-20 — Подготовить текст диплома и демонстрационный комплект
- after: T-19
- files: `docs/{architecture,runbook,demo,results}.md`, `README.md`, `reports/figures/`; материалы диплома в отдельном согласованном каталоге ATS
- estimate: 4–6 дней, черновики готовятся с T-1
- acceptance: GIVEN реализация и измерения WHEN собирается комплект защиты THEN есть схема архитектуры, таблица преемственности курсовой, методика и результаты E-1–E-5, ограничения, инструкция чистого запуска и сценарий демонстрации от версии данных до отката. Каждое количественное утверждение связано с исходным отчётом. Полный demonstration run выполняется из новой копии по README без скрытых ручных настроек.
- check: ручной L5 по инструкции другим читателем либо чистой сессией; сверка ссылок и чисел с отчётами; демонстрация успешного цикла и одного отказа с восстановлением; итоговый checklist R-1–R-8.
- satisfies: R-8

## Соответствие требований задачам

| Требование | Основные задачи |
|---|---|
| R-1 | T-2, T-3, T-4, T-7, T-8, T-9, T-11, T-12, T-18, T-19 |
| R-2 | T-3, T-5, T-9, T-10, T-17, T-18 |
| R-3 | T-4, T-7, T-8, T-9, T-10, T-17, T-18 |
| R-4 | T-2, T-6, T-7, T-12, T-18 |
| R-5 | T-11, T-13, T-17, T-18 |
| R-6 | T-15, T-16, T-17, T-18 |
| R-7 | T-2, T-14, T-18 |
| R-8 | T-1, T-3, T-6, T-16, T-18, T-19, T-20 |
