# Версии и проверка совместимости T-2

Дата: 26.10.2026. Целевая среда: Linux arm64, CPython 3.11, Debian Bookworm. **Resolver пройден; Docker smoke пока не завершён.** Метаданные и синтаксические проверки не считаются доказательством совместного исполнения.

## Зафиксированные входы

| Контур | Версия/идентичность |
|---|---|
| ML runtime | `python:3.11.17-slim-bookworm`, index digest `sha256:f06d01d4ba3f8ad5af45a3ec87df19c1d317d8e8961edc0fabda61811af6f7cf` |
| Airflow отдельно | `apache/airflow:3.3.2-python3.11`, index digest `sha256:a6685a7a708ed6fa910c2c05458d7f2b0db9cca5c3bf58a72ded389767ad3e65` |
| PostgreSQL | `18.3-alpine`, index digest `sha256:54451ecb8ab38c24c3ec123f2fd501303a3a1856a5c66e98cecf2460d5e1e9d7` |
| ML библиотеки | MLflow 3.17.2, BentoML 1.4.40, GX 1.24.1, DVC 3.67.1/DVC-S3 3.3.0, Optuna 5.0.0 |
| Численные библиотеки | scikit-learn 1.9.1, NumPy 2.4.6, pandas 3.0.5, SciPy 1.17.1, PyArrow 25.0.0 |
| S3 | boto3/botocore 1.43.117, aiobotocore 3.10.0, s3fs/fsspec 2026.9.0 |
| MinIO community | Source commit `9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a`, release source tag `RELEASE.2025-10-15T17-29-55Z` |
| MinIO source archive | SHA-256 `45521908307306e925c98d629e1c17d78c8b72b6ee242b1bfb1409f7d8ee5841` |
| MinIO compiler | `golang:1.26.5-alpine3.24`, digest `sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2`; `GOTOOLCHAIN=local`, `CGO_ENABLED=0`, ограниченный параллелизм |

ML зависимости разрешены в `requirements-ml.lock`: 202 точных pins и хеши разрешённых артефактов. SHA-256 файла: `5598e0853818e5531334bdeeff3dbc1b5c7dfd818e4dbfb2602622df7b165eee`. Build tools выделены в `requirements-build.lock`. Airflow не устанавливается в ML venv: его окружение закреплено отдельным образом; ML venv копируется в этот образ как `/opt/ml-venv`.

У DVC есть транзитивный source-only `antlr4-python3-runtime==4.9.3`. Для него фиксируется хеш source distribution и используются отдельно закреплённые setuptools/wheel; остальные ML пакеты устанавливаются из wheels. Build isolation отключён после установки закреплённых build tools, чтобы не загрузить неучтённые зависимости сборки.

## Найденные ограничения

- `aarch64-unknown-linux-gnu` у установленного uv 0.5.9 означает manylinux 2.17 и исключает новые ARM64 wheels. Lock разрешён для `aarch64-manylinux_2_36`, соответствующего Bookworm; глобальные uv/Python не обновлялись.
- Latest NumPy/SciPy с требованием Python 3.12 не включены. Библиотеки выбраны по наличию CPython 3.11 ARM64 wheels и ограничениям общего графа.
- boto3 1.43.118 конфликтует с ограничением botocore у aiobotocore 3.10.0; согласованная пара здесь — 1.43.117.
- MinIO community распространяется как исходный код; проверенный старый публичный Docker Hub tag недоступен. `docker/minio.Dockerfile` собирает выбранный community commit самостоятельно, проверяя архив и сохраняя upstream LICENSE. Это собственная сборка исходников MinIO, без замены на другой продукт.

## Реальный compatibility smoke

Запуск из корня репозитория:

```sh
ATS_RUN_COMPATIBILITY=1 python3 -m unittest discover -s tests/integration -p test_compatibility.py
```

Код должен быть закоммичен: задача Airflow клонирует точный Git SHA из read-only mount. Изменение lockfile требует пересборки ML/Airflow images. Отсутствие opt-in приводит к явному `skip` и **не закрывает T-2**.

Тест генерирует runtime credentials в игнорируемом `.local/compat/<run_id>/`, поднимает только отдельный Compose-проект и проверяет:

1. Реальную задачу LocalExecutor через Airflow REST API, отдельный checkout и совпадение lock hash с образом.
2. Положительную/отрицательную GX validation и DVC push → удаление локального файла/кеша → pull из MinIO.
3. Новое обучение tiny numerical fixture, MLflow run и точную зарегистрированную версию, импорт в BentoML.
4. Prediction из отдельного serving-контейнера без bind mount исходного репозитория и без S3/registry credentials.
5. Отрицательный DAG с несовпадающим lock hash: ошибка до worker, без успешного training report.

Отдельный unit regression использует настоящий временный Git-репозиторий и разные файлы checkout/image lock без expected-hash override. Он подтверждает, что основное сравнение файлов блокирует worker; удаление этого сравнения делает тест красным.

Первый Docker-прогон дошёл до startup и обнаружил отказ исполнения PostgreSQL init-скрипта из host bind mount. В исправленной fixture bootstrap копируется в собственный образ от закреплённого PostgreSQL с режимом 0644; служебные credentials остаются только в runtime environment. Неуспешный прогон и успешная очистка сохранены в локальных evidence.

Следующий прогон подтвердил startup PostgreSQL/MLflow и обнаружил требование SimpleAuthManager открывать собственный password store в режиме записи. Airflow получает внутренний том auth с правами владельца и файлом 0600; исходный файл хоста по-прежнему подключён read-only только к bootstrap. Логи старта сохраняются отдельно от итогового диагностического фрагмента, чтобы сообщения DAG processor не скрывали причину ошибки API.

Сохраняются очищенные от секретов отчёты и диагностические логи в `.local/compat/<run_id>/`. В `finally` удаляются только контейнеры/volumes уникального тестового проекта; чужие контейнеры и образы не удаляются. Это временная compatibility инфраструктура, а не постоянный контур T-4. Root credentials MinIO используются только в её генерируемой синтетической фикстуре; постоянное разграничение DVC/MLflow выполняется в T-4.

## Первичные источники

- [Airflow prerequisites 3.3.2](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/prerequisites.html) — поддержка Python и PostgreSQL; [официальные constraints Python 3.11](https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt) — справочный граф отдельного Airflow окружения.
- [MLflow sklearn API](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.sklearn.html) и [BentoML MLflow](https://docs.bentoml.com/en/latest/reference/bentoml/frameworks/mlflow.html) — явный формат сериализации, импорт точной модели и локальная загрузка.
- [GX dataframe API](https://docs.greatexpectations.io/docs/core/connect_to_data/dataframes/) — runtime batch для pandas и проверка Expectations.
- [MinIO source](https://github.com/minio/minio/tree/9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a) — community исходники, условия и версия сборки.
- [uv 0.5.9 platform mapping](https://github.com/astral-sh/uv/blob/0.5.9/crates/uv-configuration/src/target_triple.rs) — явное разрешение Linux/glibc target.

Результаты Docker smoke будут зафиксированы после фактического исполнения. Полные модели и итоговые эксперименты диплома на этом этапе не создаются.
