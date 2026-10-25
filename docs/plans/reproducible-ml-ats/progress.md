# Progress: реализация диплома по автоматизации жизненного цикла ML в ATS

> Plan: ./plan.md · Tasks: ./tasks.md. Планирование не означает выполнения задач реализации.

## Status
- [x] T-1 — Постановка и протокол
- [x] T-2 — Каркас и совместимость
- [ ] T-3 — Данные и разбиение
- [ ] T-4 — MinIO и MLflow
- [ ] T-5 — Качество данных
- [ ] T-6 — Baseline и метрики
- [ ] T-7 — Модель и происхождение
- [ ] T-8 — Optuna
- [ ] T-9 — DVC DAG
- [ ] T-10 — Airflow
- [ ] T-11 — Gate кандидата
- [ ] T-12 — BentoML API
- [ ] T-13 — Выпуск и откат
- [ ] T-14 — GitHub Actions
- [ ] T-15 — Prometheus/Grafana
- [ ] T-16 — Evidently и обратная связь
- [ ] T-17 — Переобучение
- [ ] T-18 — Приёмка
- [ ] T-19 — Эксперименты
- [ ] T-20 — Диплом и демонстрация

## Learnings
<!-- Добавлять наблюдения по мере выполнения задач. -->

- T-2: MinIO community собран из проверенного source commit; PostgreSQL init включён в image, Airflow auth store требует writable внутреннего volume. Реальный LocalExecutor/GX/DVC/MLflow/Bento smoke и cleanup прошли; отчёт reports/compatibility/t2.json. Отрицательный lock guard дополнительно проверен реальной Git unit fixture.

## Execution

- Epic #1; T-N corresponds to issue N+1.
- T-1 input decisions and the single README bootstrap approved on 26.10.2026.
- T-1 documents accepted and checked: R-1–R-8/E-1–E-5 coverage, relative links, authorization evidence and absence of private paths; publication through the issue-2 PR.
