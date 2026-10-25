# Локальный постоянный core

Core содержит PostgreSQL 18.3, MinIO и MLflow 3.17.2/Registry. Версии ML и сервера MinIO закреплены в [versions.md](versions.md); T-4 добавляет официальный клиент [MinIO mc](https://github.com/minio/mc/tree/7394ce0dd2a80935aded936b09fa12cbb3cb8096), release `RELEASE.2025-08-13T08-35-41Z`. SHA-256 его исходного архива — `95cd293c7119f16921a6dc515a1fb74a2227f19fd994b9c8b770a154e802ac44`; Docker проверяет checksum и собирает клиент закреплённым Go image из T-2. Копия upstream лицензии включена в образ. Новый внешний сервис не требуется.

Из корня репозитория:

```sh
python3 scripts/bootstrap_core.py init
python3 scripts/bootstrap_core.py config
python3 scripts/bootstrap_core.py up
python3 scripts/bootstrap_core.py status
```

Первый `up` собирает образы; MinIO server/client собираются из upstream исходников с ограничением двух Go workers. ML venv и Airflow venv разделены, core использует только ML image. Docker должен иметь свободную память для лимитов core: PostgreSQL 512 MiB, MinIO 1 GiB, MLflow 2 GiB; это лимиты, не измеренное потребление. Первый успешный smoke показал MLflow 1.499 GiB при лимите 1.5 GiB, поэтому предел увеличен до 2 GiB и проверен повторно. При добавлении Airflow и observability нужно учитывать суммарный запас Docker VM; эти сервисы пока не входят в core.

Генератор создаёт случайные credentials в игнорируемом `.local/core/core.env` (0600), каталог имеет режим 0700. Повторный запуск сохраняет их. Не заменяйте файл при существующих volumes: PostgreSQL применяет init-скрипт только к пустому каталогу, а существующие пользователи должны сохранять прежние пароли. Для переноса стенда нужны и volumes, и приватная конфигурация. Не публикуйте `docker compose config`, `docker inspect`, полный env или MinIO root credentials: они доступны доверенному локальному владельцу Docker. Для проверки конфигурации используйте `config --quiet`.

Адреса по умолчанию: MLflow `http://127.0.0.1:5050`, MinIO S3 `http://127.0.0.1:9000`, console `http://127.0.0.1:9001`. Порты публикуются только на loopback, PostgreSQL не имеет host port. Перед первым `init` занятые порты можно заменить флагами `--minio-port`, `--console-port`, `--mlflow-port`; последующие команды используют сохранённые значения. Это локальный доверенный стенд без TLS/MLflow login; не открывайте порты во внешнюю сеть и не добавляйте публичный reverse proxy.

Для MLflow client достаточно источника `.local/core/mlflow.env` и tracking URI; артефакты проходят через MLflow proxy. Сервер получает только IAM identity для `ats-mlflow`. DVC identity имеет доступ только к `ats-dvc`, не получает MinIO admin или MLflow bucket. Root identity доступна MinIO и одноразовому `storage-init`, чья mc конфигурация находится в tmpfs. Используется [официальный policy attach](https://min.io/docs/minio/linux/reference/minio-mc-admin/mc-admin-policy-attach.html). PostgreSQL роли `mlflow`/`airflow` имеют отдельные БД, не являются superuser/createdb/createrole; PUBLIC connect отозван. Airflow пока не запущен, его отдельная БД подготовлена для T-10.

Когда T-3 metadata уже находится в checkout, настройте DVC client с установленным закреплённым ML окружением:

```sh
set -a
. .local/core/dvc.env
set +a
dvc remote add --local --default core s3://ats-dvc/data
dvc remote modify --local core endpointurl http://127.0.0.1:9000
dvc push
```

Секреты остаются в локальном env; `.dvc/config.local` игнорируется. В контейнерной сети endpoint — `http://minio:9000`; host loopback endpoint из контейнера использовать нельзя. `.local/core/client.json` хранит оба адреса и URI без credentials. Настройку remote не включают в публикуемые `.dvc/config` вместе с секретами.

```sh
python3 scripts/bootstrap_core.py restart
python3 scripts/bootstrap_core.py down
python3 scripts/bootstrap_core.py up
```

`restart` и `down` сохраняют named volumes `postgres-data`/`minio-data`. Команды с `down --volumes` удаляют данные: bootstrap CLI такой команды не предоставляет. Удаление volumes используется только в изолированном integration test с уникальным project name. Смена image не является миграцией схемы PostgreSQL; перед будущими обновлениями нужны backup и проверка восстановления в отдельном проекте.

Реальная проверка T-4:

```sh
ATS_RUN_STORAGE_TRACKING=1 python3 -m unittest discover -s tests/integration -p test_storage_tracking.py -v
```

Тест использует отдельный случайный project/порты/volumes и синтетическую fixture, выполняет настоящий DVC push, удаляет локальный payload/cache и восстанавливает pull; сохраняет run/artifact/зарегистрированную модель в MLflow с SQL backend; проверяет чтение после restart и `down`→`up` без удаления volumes. Дополнительно проверяются S3 cross-bucket read/write/list denials, неверные S3/SQL credentials, SQL роли/межбазовые запреты, loopback bindings и отказ MLflow для недоверенного Host. В finally удаляются только ресурсы этого тестового проекта, чужие контейнеры и образы не удаляются. Санитизированные evidence/logs и ресурсный snapshot сохраняются в `.local/core-tests/<run_id>/`; `skip` без opt-in не является прохождением. Тестовое время включает инфраструктуру и не является E-2 или временем обучения ATS.
