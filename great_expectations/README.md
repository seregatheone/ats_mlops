# Data quality boundary (GX 1.24.1)

The single source of truth is packaged in
[`src/ats_mlops/data/expectations/`](../src/ats_mlops/data/expectations/).
`pairs.json` is a real Great Expectations suite. `checkpoint.json` defines the
checkpoint instantiated by `ats_mlops.data.validate`: ephemeral pandas source,
whole-dataframe batch definition, stored suite, validation definition, checkpoint.
The JSON files ship in the wheel and are loaded with `importlib.resources`, so
validation also works in the src-only runtime image and outside a checkout.
All 17 expectations run on raw JSONL rows before `clean_rows` can remove an exact
duplicate pair. Schema/type predicates are explicit derived columns; the
expectations evaluate them in GX. The relevance predicate distinguishes integers
from Python booleans/floats and rejects non-finite values. T3 identity, hash,
canonical-content and split/component audits supplement the GX expectations.

Run in the pinned Python 3.11 ML environment (GX 1.24.1 from
`requirements-ml.lock`):

```sh
python -m ats_mlops.cli data validate --manifest data_manifest.json --report-dir reports
python -m ats_mlops.cli data validate --mode training --manifest data_manifest.json \
  --admission-report reports/data-admission.json --report-dir reports
python -m unittest discover -s tests/unit -p test_data_validation.py
```

Admission opens the original source before cleaning and audits the complete
frozen dataset, including test. It writes `data-admission.json` and
`data-admission.md`; a duplicate retained in T3's cleaning receipt is still an
admission failure here. Training mode requires a successful admission receipt
bound to the exact manifest and current validation configuration. Receipt schema,
engine version, report fingerprint and all required successful checks are
verified. The fingerprint detects accidental changes; it is not a cryptographic
signature against an attacker who can rewrite both data and receipts.

Training mode opens only train/validation artifact bytes; it validates metadata
inventories for all splits without opening test/source/corpus/split-list files.
The same raw suite checks run on the requested partitions before the T3 loader.
The preflight returns the exact checked rows and rejects a change between reads.

The T7 training entry point must use this fail-closed boundary before fitting:

```python
from pathlib import Path
from ats_mlops.data.validate import require_valid

rows = require_valid(Path("data_manifest.json"), Path("reports"),
                     admission_report=Path("reports/data-admission.json"))
# Fit only rows["train"]; evaluate/tune using rows["validation"].
```

A failed preflight persists `data-training.json`/`.md` and raises
`ValidationError`; the caller must let it abort fitting. T5 tests this boundary,
while the concrete fit-stage guard belongs to T7. Both CLI modes exit 1 on
rejection and print the structured JSON report. GX execution/version failures
fail closed.

No GX data docs or raw validation results are exported: `BOOLEAN_ONLY` execution
and an explicit report allowlist keep texts, contacts, unexpected values and
exception messages out of reports and CLI output. Reports contain check names,
reason codes, row counts and hashes. Ephemeral GX state is discarded. Run
artifacts should stay local or be handled by the later pipeline stages; the
negative fixtures are synthetic CI-only cases.
