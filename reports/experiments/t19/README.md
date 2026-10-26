# T19: публичные агрегаты формальных исследований

[aggregates.json](aggregates.json) — проверенная scalar projection основной
кампании `cwd2` на синтетике. E1/E3/E4/E5 passed, E2 not_run (0/5 настоящих
человеческих пар), общий статус **incomplete**. Основные выводы:
[docs/results.md](../../../docs/results.md).

Публичный файл не содержит исходных текстов, candidate/vacancy inventories,
private paths или credentials. Для полного аудита native происхождения
нужны сохранённые private receipts/artifacts. Пересборка из этого файла
повторяет только таблицы и рисунки; это не replay экспериментов.

## Происхождение

| Связь | SHA256 / Git |
|---|---|
| Публичный aggregate input | `e5396e1bfece219ff5f569ebdf7c93eb3b906702da9640406205d3d661e86ee0` |
| Исполняемый source исследований | `14be8edffb68a2320404e8e1a03f37a740694466` |
| Training source FIRST S1 | `01ccf13d123857ea7b0c8c36fef6c29a13935b78` |
| Private freeze | `3d081dba7f13ee46366b10417b1a8f7472da64bbe62220bb5c9de090807d6481` |
| Frozen spec | `74eff657e0974f1877af04645ca017aa2184c43afee58399c7aea7cec9d6049d` |
| Renderer `scripts/rebuild_experiment_report.py` | `022c5ba779a687197f07f7633006eefc1f8aa85d20878fe533efb5af84d15f37` |
| Опубликованный figure manifest | `0d3022bf18653f6724db477321157126654bf8104121e5bc17589a09b73c935c` |

Поле `source_receipts_sha256` сохраняет хеши исходных native/operator outputs,
включая E5 numeric/commit/completed и retained campaign receipt. Это ссылки
на действительные свидетельства, а не разрешение заменять их JSON с PASS.
[Протокол](../../../docs/experiment-protocol.md) определяет объёмы серий
и границу единственной holdout попытки.

Кандидат E5 — FIRST S1 private model1/run4be0804b65034beba65023b7f8572167.
Его не заменяют версии E1/E4. Документационный и renderer code созданы после
измерений и не переатрибутируют их исполняемому source нового commit.

Первоначальная проекция с SHA `d216fda46ee24982403a1c45afface5d2948bb4266f47f3f53c0ee66d97ace2f`
сохранена отдельно. В финальной изменено только
`source_receipts_sha256.renderer.code`: renderer делает нулевые точки E1
видимыми с подписями и положительным диапазоном оси. Научные значения
и experimental source остались прежними; пустой нулевой столбец не
заменялся вымышленной ненулевой разницей.

## Раздельный учёт

- Основная кампания:4 fits =3 E1 +1 E4,18 E3 attempts,7 E4 scenarios,1 E5.
- Сохранённая остановленная кампания:3 fits,15 успешных fault trials и1
  cwd/import failure до intended crash checkpoint. Retained receipt:
  `e936e6157bc8fe7ae619e6255115276c95206cb3857442bf3db0409d57cb5d85`.
- Qualification T18:5 fits, отдельно от formal T19.
- Всего записанных private fits:12. Изолированный CI исключён.15 Registry
  versions также включают zero-fit fixtures и не являются fit denominator.

Основные таблицы не объединяют successful outcomes двух кампаний.
Два внешних observer guard отказа E4 до grant/intent/slot не считались
experiment attempts; frozen source/spec/environment не менялись.

## Что опубликовано

[Сгенерированные таблицы](../../figures/t19/results.md) и
[manifest](../../figures/t19/manifest.json) связаны с тем же aggregate SHA.
Рисунки доступны в SVG и PNG:

| Рисунок | Смысл |
|---|---|
| [E1 reproducibility](../../figures/t19/E1-reproducibility.svg) | Max score difference каждого из3 fresh fits |
| [E3 outage bounds](../../figures/t19/E3-outage-bounds.svg) | Средние верхние границы по конечным HTTP-пробам |
| [E4 effects](../../figures/t19/E4-effects.svg) | Новые requests и fits/runs в семи сценариях |
| [E5 quality difference](../../figures/t19/E5-quality-difference.svg) | Model−baseline и парный percentile95% |
| [E5 latency](../../figures/t19/E5-latency.svg) | Median/P95 одного прохода на группу, ms |

E5:12 eligible groups/240 pairs, обе NDCG@10 и Recall@10 равны1 у baseline
и FIRST S1. Разности0, интервалы[0;0]. Преимущество ML по качеству не
наблюдалось; реальное hiring quality не установлено. Latency —
12 наблюдений, не нагрузочный benchmark сервиса. E2 графика нет.

## Пересборка без inference

Из корня committed checkout можно проверить schema и создать таблицы
stdlib Python, не импортируя модель, native adapters, ledger или данные:

```sh
python3 -B scripts/rebuild_experiment_report.py --input reports/experiments/t19/aggregates.json
python3 -B scripts/rebuild_experiment_report.py --input reports/experiments/t19/aggregates.json --output /tmp/ats-t19-tables-NEW
```

Output directory должен отсутствовать. Для `--figures` требуется уже
подготовленный Matplotlib из закреплённого ML окружения; script ничего не
устанавливает. Фактическая публикация использовала image
`sha256:574c48deb107cba3f0bdc982d3aa0ab1500de58ab1694052bb92efbcab13ec91`.
Если этот образ уже доступен локально, offline запуск монтирует только
renderer/aggregate и отдельный writable output:

```sh
T19_REPO="$(pwd -P)"
T19_REPORT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ats-t19-report.XXXXXX")"
T19_REPORT_IMAGE="sha256:574c48deb107cba3f0bdc982d3aa0ab1500de58ab1694052bb92efbcab13ec91"
docker run --rm --pull=never --network none --read-only --cpus 1 --memory 768m \
  --user "$(id -u):$(id -g)" --tmpfs /tmp:rw,size=64m,mode=1777 \
  -e HOME=/tmp -e MPLCONFIGDIR=/tmp/mpl \
  --mount "type=bind,source=$T19_REPO/scripts/rebuild_experiment_report.py,target=/renderer.py,readonly" \
  --mount "type=bind,source=$T19_REPO/reports/experiments/t19/aggregates.json,target=/aggregates.json,readonly" \
  --mount "type=bind,source=$T19_REPORT_DIR,target=/output" \
  --entrypoint /opt/ml-venv/bin/python "$T19_REPORT_IMAGE" -I -B /renderer.py \
  --input /aggregates.json --output /output/rebuilt --figures
```

Credentials, source checkout, original test и Docker socket контейнеру не
передаются. При отсутствии образа этот command не скачивает иной `latest`;
окружение требуется подготовить отдельно по locks/recipe проекта.
Manifest проверяет опубликованные байты. В другом plotting окружении
оформление может отличаться, однако исходные значения таблиц и input SHA
должны совпасть. Создание графика не создаёт новый E5 read/scoring и не
снимает общий incomplete status из-за отсутствующего E2.
