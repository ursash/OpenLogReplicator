"""Frozen contract pins for Live SRL E2E."""

CONTRACT_VERSION = "20261012T031500Z-v1"
EXPECTED_OPS_CONTRACT_VERSION = "20261012T031500Z-expected-ops-v3"
EXPECTED_OPS_FINAL_STATE_VERSIONS = (
    "20261012T031500Z-expected-ops-v2",
    "20261012T031500Z-expected-ops-v3",
)
FINAL_STATE_RECORD_TYPE = "final_key_state"
RUN_ID = "20261012T031500Z"
RECEIPT_CONTRACT_VERSION = "20261012T031500Z-receipt-v1"
MEASUREMENT_WINDOW_VERSION = "20261012T031500Z-measure-v1"
PROVENANCE_EVIDENCE_VERSION = "20261012T031500Z-provenance-v1"
RECEIPT_REQUIRED_FIELDS = ("run_id", "phase", "receipt_type", "utc", "host")
RECEIPT_TYPES = frozenset({"phase_boundary", "sql_commit", "key_anchor", "barrier"})
REQUIRED_SOURCE_SCHEMA = "OLR_RAC_TEST"
REQUIRED_CAPTURE_ISOLATION = "read_committed"
TOMBSTONES_ON_DELETE_DEFAULT = False

LANE_REGEX = {
    "baseline": r"^ocr370\.live\.srl\.20261012T031500Z\.baseline(\.transaction|\.OLR_RAC_TEST\..+)?$",
    "recovery": r"^ocr370\.live\.srl\.20261012T031500Z\.recovery(\.transaction|\.OLR_RAC_TEST\..+)?$",
}

BUSINESS_TABLES = ("EV", "PP")
TABLE_COLUMNS = {
    "EV": ("ID", "INST", "TAG", "VAL", "PAD"),
    "PP": ("ID", "CTR", "INST", "NOTE"),
}
INT_COLUMNS = {
    "EV": frozenset({"ID", "INST", "VAL"}),
    "PP": frozenset({"ID", "CTR", "INST"}),
}
STRING_COLUMNS = {
    "EV": frozenset({"TAG", "PAD"}),
    "PP": frozenset({"NOTE"}),
}

EXCLUDED_DML_PHASES = ("P099", "P104")

CAPTURE_ROW_REQUIRED = (
    "topic",
    "partition",
    "kafka_offset",
    "timestamp",
    "key_raw",
    "value_raw",
)

FORBIDDEN_TAGS = ("ROLLED_BACK", "PARTIAL_RB")
JOURNAL_REQUIRED_FIELDS = ("run_id", "event_id", "utc", "op")

RECORD_KAFKA = "kafka_committed_event"
RECORD_FORBIDDEN = "forbidden_uncommitted"
