# Источники методического черновика

Проверено 26.10.2026 по первичным публикациям и авторским репозиториям. Ниже приведены пять внешних работ; прямых цитат в черновике нет. Год журнальной публикации отделён от года препринта. DOI не восстанавливался по памяти, отсутствие DOI на странице не означает его отсутствия вообще.

## И1

D. Sculley, Gary Holt, Daniel Golovin, Eugene Davydov, Todd Phillips, Dietmar Ebner, Vinay Chaudhary, Michael Young, Jean-François Crespo, Dan Dennison. **Hidden Technical Debt in Machine Learning Systems**. Advances in Neural Information Processing Systems 28, 2015. [Первичная запись proceedings](https://papers.nips.cc/paper/2015/hash/86df7dcfd896fcaf2674f757a2463eba-Abstract.html). DOI в этой записи не указан.

Использование: системные зависимости и риски сопровождения ML. Работа не является доказательством обнаруженных в нашем стенде отказов или величины затрат.

## И2

Dominik Kreuzberger, Niklas Kühl, Sebastian Hirschl. **Machine Learning Operations (MLOps): Overview, Definition, and Architecture**. IEEE Access, 11, 2023, pp. 31866–31879. DOI: [10.1109/ACCESS.2023.3262138](https://doi.org/10.1109/ACCESS.2023.3262138). Название, авторы и выходные данные сверены с [репозиторием университета автора](https://eref.uni-bayreuth.de/id/eprint/76163/); [авторский препринт](https://arxiv.org/abs/2205.02302) опубликован в 2022 году.

Использование: компоненты, роли и процессы MLOps. Обзор не определяет единственный правильный стек и не подтверждает производительность нашей реализации.

## И3

Joelle Pineau, Philippe Vincent-Lamarre, Koustuv Sinha, Vincent Lariviere, Alina Beygelzimer, Florence d'Alche-Buc, Emily Fox, Hugo Larochelle. **Improving Reproducibility in Machine Learning Research (A Report from the NeurIPS 2019 Reproducibility Program)**. Journal of Machine Learning Research, 22(164), 2021, pp. 1–20. [Первичная журнальная запись](https://jmlr.org/papers/v22/20-303.html). DOI на странице не указан.

Использование: раскрытие условий, кода и процедуры воспроизведения. Конкретные допуски численного сравнения и три повтора E-1 определены протоколом проекта, а не приписаны этой статье.

## И4

Margaret Mitchell, Simone Wu, Andrew Zaldivar, Parker Barnes, Lucy Vasserman, Ben Hutchinson, Elena Spitzer, Inioluwa Deborah Raji, Timnit Gebru. **Model Cards for Model Reporting**. FAT* '19: Conference on Fairness, Accountability, and Transparency, 2019. DOI: [10.1145/3287560.3287596](https://doi.org/10.1145/3287560.3287596). Авторы, название, конференция и связанный DOI проверены по [авторской версии](https://arxiv.org/abs/1810.03993), первоначально опубликованной в 2018 году.

Использование: документирование назначения и ограничений модели. Предложенный формат не подтверждает справедливость ранжирования и не заменяет оценку на независимых данных.

## И5

Timnit Gebru, Jamie Morgenstern, Briana Vecchione, Jennifer Wortman Vaughan, Hanna Wallach, Hal Daumé III, Kate Crawford. **Datasheets for Datasets**. Авторская версия v8, 2021; первоначальный препринт — 2018. DOI авторского препринта: [10.48550/arXiv.1803.09010](https://doi.org/10.48550/arXiv.1803.09010). [Первичная авторская запись](https://arxiv.org/abs/1803.09010) указывает публикацию в Communications of the ACM в декабре 2021 года. Здесь приведён DOI препринта, не журнальной статьи.

Использование: происхождение, состав и допустимое применение данных. Карточка синтетического корпуса не превращает метки генератора в независимые сведения о найме.

## Принятые технические свидетельства

Срез технических свидетельств — принятый main `034f5371b297076fe2f6f7f82defba626fb8ea44`. Ссылки ниже закреплены на этом commit; текстовый черновик в своей ветке не переатрибутирует исполнение новым source SHA. Файлы проверены по сохранённым Git objects, без запуска модели, контейнеров или чтения исходного test. Сокращённые SHA в таблице служат навигацией; полные значения приведены ниже.

| Свидетельство | Отчёт и связь с raw | Что оно позволяет утверждать |
|---|---|---|
| Исправление #36 | [data-reservations.json](https://github.com/seregatheone/ats_mlops/blob/034f5371b297076fe2f6f7f82defba626fb8ea44/reports/data-reservations.json), SHA `1ad41870…` | Зафиксированы root policy, исходные partition SHA, no-fit guards и отдельные последующие integration gates; не оценка качества старых моделей |
| T-16 batch monitoring | [drift.json](https://github.com/seregatheone/ats_mlops/blob/034f5371b297076fe2f6f7f82defba626fb8ea44/reports/monitoring/drift.json), SHA `943aae09…`; actual receipt SHA `8805f486…` | Реальные JSON/HTML и отдельные calibration/window/feedback/completeness проверки. Batch source d13c70c, serving source7235b01, runner87f43fc различены |
| T-17 замкнутый цикл | [t17.json](https://github.com/seregatheone/ats_mlops/blob/034f5371b297076fe2f6f7f82defba626fb8ea44/reports/retraining/t17.json), SHA `97b8b0e7…`; publication связывает 80 exact raw/public backing files | Один цикл от двух окон через независимый synthetic v2 и обычный fit до A3/reference/cooldown; исполняемый source48b6d3a |
| T-17 происхождение обучения | [native repro](https://github.com/seregatheone/ats_mlops/blob/034f5371b297076fe2f6f7f82defba626fb8ea44/reports/retraining/t17/native-training/repro.json), [run manifest](https://github.com/seregatheone/ats_mlops/blob/034f5371b297076fe2f6f7f82defba626fb8ea44/reports/retraining/t17/lineage/run-manifest.json) | Один обычный `dvc repro`, один train event, модель14/run `a37686ddac5f44b0847f402ff8ab911f`; не forced replica E-1 |
| T-17 состояние после выпуска | [final-state.json](https://github.com/seregatheone/ats_mlops/blob/034f5371b297076fe2f6f7f82defba626fb8ea44/reports/retraining/t17/operator/final-state.json), SHA `21d3f288…` | Сохранённое ACTIVE14/generation4, одна заявка и отсутствие inflight; это снимок конкретного момента, не обещание бессрочной доступности |

Полные SHA256 проверенных summary:

- #36: `1ad41870bca40ea0a240a3586687615abd8af1d0a5491777ff2d3a928a0995e5`;
- T-16: `943aae09a8dfb90712088e515f7da8a1abeea584ec22f4af99a09401f955c772`;
- T-17: `97b8b0e7fd3d826f14cfb4fce5360f9b1821947b9acb87cba52875e4e7a55e58`.

Материалы читаются вместе с [границами #36](https://github.com/seregatheone/ats_mlops/blob/034f5371b297076fe2f6f7f82defba626fb8ea44/docs/semantic-reservations.md), [описанием T-16](https://github.com/seregatheone/ats_mlops/blob/034f5371b297076fe2f6f7f82defba626fb8ea44/docs/drift.md) и [описанием actual T-17](https://github.com/seregatheone/ats_mlops/blob/034f5371b297076fe2f6f7f82defba626fb8ea44/docs/retraining-evidence.md). Source887 с неудачным macOS publish и ранние частично успешные попытки T-16 сохраняют свои исходы; успешный поздний запуск не переименовывает их в PASS. Исторические4/5 пригодны для инженерной демонстрации, но не для независимого E-5 качества; новые13/14 также не превращаются в заранее выбранный финальный E-5 кандидат без T-18 freeze.

## Принятая T18 приёмка

Main commit `2df2657b5d9ceac1defd7ee1039d4131e5dfd6e1` публикует
[описание приёмки](https://github.com/seregatheone/ats_mlops/blob/2df2657b5d9ceac1defd7ee1039d4131e5dfd6e1/docs/acceptance.md),
[summary](https://github.com/seregatheone/ats_mlops/blob/2df2657b5d9ceac1defd7ee1039d4131e5dfd6e1/reports/acceptance/t18/summary.json),
[native audit](https://github.com/seregatheone/ats_mlops/blob/2df2657b5d9ceac1defd7ee1039d4131e5dfd6e1/reports/acceptance/t18/raw/acceptance-audit.json)
и [public hash map](https://github.com/seregatheone/ats_mlops/blob/2df2657b5d9ceac1defd7ee1039d4131e5dfd6e1/reports/acceptance/t18/public-hashmap.json).
Восемь cases S1–S8 имеют PASS. Private freeze SHA256 —
`3d081dba7f13ee46366b10417b1a8f7472da64bbe62220bb5c9de090807d6481`;
публичная проекция не заменяет полный private backing graph.

Пять qualification fits (1 S1 + 3 S2 + 1 S6) не являются formal T19.
FIRST S1 model1 private backend/run `4be0804b65034beba65023b7f8572167`
сохраняет training source `01ccf13d123857ea7b0c8c36fef6c29a13935b78`.
S1–S5 используют этот execution source, S6–S8 —
`14be8edffb68a2320404e8e1a03f37a740694466`. Native audit проверяет historical
Git bytes/ancestry и original phase/spec/environment/authorization bindings;
исправленный negative producer атрибутирован отдельным component. Publication
commit не является новым training source. При freeze E1–E5 были `not_run`.

Основание преемственности курсовой: редакция `NIR-2_с_матрицами.docx`, SHA256
`ef5a44e18bd28cb207b9e52ea84deece47b0890f47b6fc6e9e3cbf7d22983219`,
XML-абзац265 (ядро5) и 836/841 (следующая экспериментальная проверка).
Это координаты разбора, не страницы. Расширенные заметки по четырнадцати
функциональным классам остаются отдельным основанием; их выводы не приписываются
одному сравнению DOCX. Библиографические реквизиты курсовой не выдумываются.

## Внутренние основания и следующий этап

[Требования](requirements.md), [решения](decisions.md), [контракт данных](data-contract.md), [протокол](experiment-protocol.md) и [исходный план](plans/reproducible-ml-ats/plan.md) задают постановку и критерии работы. Они не заменяют первичные результаты. Архитектура описана в [architecture.md](architecture.md), границы и будущая публикация формальных измерений — в [results.md](results.md). Подтверждённые T19 raw/numeric ещё должны быть перенесены в эту редакцию; human E2 отсутствует. Следующее дополнение связывает каждое значение с exact raw/results/rebuild, сохраняя исходы старых опытов. Этот checkpoint не завершает T19/T20 и не является окончательным дипломом.
