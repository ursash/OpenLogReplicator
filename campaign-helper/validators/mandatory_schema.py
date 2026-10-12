"""Mandatory complete-object schemas (exact key sets, no optional-subset acceptance)."""

from __future__ import annotations

from typing import Any

QEV_REQUIRED_KEYS = frozenset(
    {
        "contract_version",
        "run_id",
        "mode",
        "pin_ref",
        "dml_artifact",
        "final_state_artifact",
        "pin_sha256",
        "dml_sha256",
        "final_state_sha256",
        "expected_committed_counts",
        "expected_forbidden_counts",
        "expected_committed_table_counts",
        "phase_receipts",
        "capture_manifest_requirements",
    }
)

CAPTURE_MANIFEST_REQUIRED_KEYS = frozenset(
    {"complete", "records", "isolation_level", "partitions", "expect_records"}
)

PARTITION_REQUIRED_KEYS = frozenset(
    {
        "topic",
        "partition",
        "beginning",
        "end",
        "final_position",
        "captured",
        "first_offset",
        "last_offset",
    }
)

MW_REQUIRED_TOP_KEYS = frozenset(
    {
        "run_id",
        "contract_version",
        "source_measured_dml_from_utc",
        "source_phases_excluded",
        "source_phases_in_oracle",
        "consumer_scope",
        "lanes",
        "partition_capture_bounds",
        "artifact_hashes",
    }
)

LANE_SLOT_REQUIRED_KEYS = frozenset(
    {
        "consumer_start_utc",
        "catch_up_after_p099_utc",
        "p099_catch_up_position",
    }
)

SOURCE_STOCK37_REQUIRED = frozenset({"db", "schema", "table", "txId", "scn"})


def missing_keys(obj: dict[str, Any], required: frozenset[str]) -> list[str]:
    return sorted(k for k in required if k not in obj)


def extra_keys(obj: dict[str, Any], allowed: frozenset[str]) -> list[str]:
    return sorted(k for k in obj if k not in allowed)


def issue(code: str, detail: str) -> dict[str, str]:
    return {"code": code, "detail": detail}
