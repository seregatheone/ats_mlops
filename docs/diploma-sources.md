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

## Внутренние основания и следующий этап

[Требования](requirements.md), [решения](decisions.md), [контракт данных](data-contract.md), [протокол](experiment-protocol.md) и [исходный план](plans/reproducible-ml-ats/plan.md) задают постановку и критерии работы. Они не заменяют первичные результаты. Происхождение ядра курсовой и расширенных заметок различено в плане; их библиографические реквизиты здесь не придуманы. Перед окончательным оформлением требуется подтвердить цитируемую редакцию курсовой. После E-1–E-5 список внутренних свидетельств дополняется точными отчётами и версиями, без изменения смысла уже выполненных опытов.
