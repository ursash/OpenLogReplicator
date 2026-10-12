"""External PROVENANCE: artifact-backed cross-bind (not evidence_class alone)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from srl_contracts import PROVENANCE_EVIDENCE_VERSION, REQUIRED_SOURCE_SCHEMA, RUN_ID, TABLE_COLUMNS
from srl_fields import compare_rows, materialize_oracle_row_for_table
from srl_oracle import table_short
from srl_dates import parse_utc_strict
from srl_kafka import ParsedBusinessOp
from srl_oracle import ExpectedOracle
from srl_match import ops_match
from srl_provenance_artifacts import resolve_artifact_record
from srl_receipt import ExecutionReceipt
from srl_xid import parse_stock37_tx_id, verify_oracle_xid_binding

PRIMARY_CLASS = "primary_stock37"
COMPONENT_CLASS = "synthetic_component"

# Independent Oracle only (redo / LogMiner). No OLR END / non-CDC stream as commit oracle.
ORACLE_COMMIT_SCN_SOURCES = frozenset({"oracle_redo_commit", "oracle_logminer_commit"})
CHECKPOINT_SCN_SOURCES = frozenset({"kafka_envelope", "olr_checkpoint"})
GROWTH_STATES = frozenset({"open", "growing", "archiving", "archived"})

COMMIT_RECORD_REQUIRED_KEYS = (
    "lane",
    "schema",
    "table",
    "key",
    "op",
    "before",
    "after",
    "seq_on_key",
)

# Computed by this checker only; never upgraded from input doc labels.
COMPUTED_INDEPENDENT_SOURCE_QUALIFICATION = "UNVERIFIED"
COMPUTED_PREARCHIVE_STATUS = "UNKNOWN"
COMPUTED_CAMPAIGN_STATUS = "NOT_ACCEPTED"


def extract_untrusted_declared_qualifiers(doc: dict[str, Any]) -> dict[str, Any]:
    """Labels present in PROVENANCE evidence JSON (audit echo only, not trusted)."""
    out: dict[str, Any] = {}
    for key in ("evidence_class", "independent_source_qualification", "prearchive_status", "campaign_status"):
        if key in doc:
            out[key] = doc.get(key)
    return out


def _int_equal(a: Any, b: Any) -> bool:
    try:
        return int(a) == int(b)
    except (TypeError, ValueError):
        return False


def _compare_commit_identity_fields(
    ora: dict[str, Any],
    commit_rec: dict[str, Any],
    exp: Any,
    parsed: ParsedBusinessOp | None,
    lane: str,
    op_id: str,
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    for field in ("xidusn", "xidslot", "xidsqn", "instance_number", "thread_number"):
        cv = commit_rec.get(field)
        ov = ora.get(field)
        if cv is None:
            issues.append(
                {"code": "PROVENANCE_ORACLE_FIELD_NULL", "detail": f"{op_id}: commit artifact {field}"}
            )
            continue
        if ov is None:
            issues.append(
                {"code": "PROVENANCE_ORACLE_FIELD_NULL", "detail": f"{op_id}: binding oracle missing {field}"}
            )
        elif not _int_equal(ov, cv):
            issues.append(
                {
                    "code": "PROVENANCE_ORACLE_FIELD_MISMATCH",
                    "detail": f"{op_id}: binding oracle {field} != commit artifact",
                }
            )
    raw_o = ora.get("raw_xid_bytes_hex")
    raw_c = commit_rec.get("raw_xid_bytes_hex")
    if raw_c is not None:
        if raw_o is None:
            issues.append(
                {"code": "PROVENANCE_ORACLE_FIELD_NULL", "detail": f"{op_id}: binding oracle missing raw_xid"}
            )
        elif str(raw_o).lower() != str(raw_c).lower():
            issues.append(
                {
                    "code": "PROVENANCE_ORACLE_FIELD_MISMATCH",
                    "detail": f"{op_id}: binding raw_xid != commit artifact",
                }
            )

    for req in COMMIT_RECORD_REQUIRED_KEYS:
        if req not in commit_rec:
            issues.append(
                {
                    "code": "PROVENANCE_COMMIT_RECORD_INCOMPLETE",
                    "detail": f"{op_id}: commit artifact missing {req}",
                }
            )
    if any(i["code"] == "PROVENANCE_COMMIT_RECORD_INCOMPLETE" for i in issues):
        return issues

    if str(commit_rec.get("lane", "")) != lane:
        issues.append({"code": "PROVENANCE_COMMIT_RECORD_MISMATCH", "detail": f"{op_id}: commit lane"})
    if str(commit_rec.get("schema", "")) != REQUIRED_SOURCE_SCHEMA:
        issues.append({"code": "PROVENANCE_COMMIT_RECORD_MISMATCH", "detail": f"{op_id}: commit schema"})
    if str(commit_rec.get("table", "")).upper() != str(exp.table).upper():
        issues.append({"code": "PROVENANCE_COMMIT_RECORD_MISMATCH", "detail": f"{op_id}: commit table"})
    if str(commit_rec.get("op", "")) != str(exp.op):
        issues.append({"code": "PROVENANCE_COMMIT_RECORD_MISMATCH", "detail": f"{op_id}: commit op"})
    if commit_rec.get("key") != exp.key:
        issues.append({"code": "PROVENANCE_COMMIT_RECORD_MISMATCH", "detail": f"{op_id}: commit key"})
    if exp.seq_on_key is not None and not _int_equal(commit_rec.get("seq_on_key"), exp.seq_on_key):
        issues.append({"code": "PROVENANCE_COMMIT_RECORD_MISMATCH", "detail": f"{op_id}: commit seq_on_key"})

    tbl = table_short(exp.table)
    cols = TABLE_COLUMNS[tbl]
    for side, exp_side, kafka_side in (
        ("before", exp.before, parsed.before if parsed else None),
        ("after", exp.after, parsed.after if parsed else None),
    ):
        rec_side = commit_rec.get(side)
        if (exp_side is None) != (rec_side is None):
            issues.append(
                {
                    "code": "PROVENANCE_COMMIT_RECORD_MISMATCH",
                    "detail": f"{op_id}: commit {side} nullness != expected op",
                }
            )
            continue
        if exp_side is not None and rec_side is not None:
            exp_mat = materialize_oracle_row_for_table(tbl, exp_side)
            rec_mat = materialize_oracle_row_for_table(tbl, rec_side)
            if compare_rows(exp_mat or {}, rec_mat or {}, tbl, cols):
                issues.append(
                    {
                        "code": "PROVENANCE_COMMIT_RECORD_MISMATCH",
                        "detail": f"{op_id}: commit {side} != expected op",
                    }
                )
        if (
            parsed is not None
            and rec_side is not None
            and kafka_side is not None
            and ops_match(exp, parsed)
        ):
            k_mat = materialize_oracle_row_for_table(tbl, kafka_side)
            r_mat = materialize_oracle_row_for_table(tbl, rec_side)
            if compare_rows(r_mat or {}, k_mat or {}, tbl, cols):
                issues.append(
                    {
                        "code": "PROVENANCE_COMMIT_RECORD_MISMATCH",
                        "detail": f"{op_id}: commit {side} != kafka capture",
                    }
                )
    return issues


def _compare_witness_binding(wit: dict[str, Any], wit_rec: dict[str, Any], op_id: str) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    for req in ("incarnation", "thread", "sequence", "member", "sampled_utc", "archive_delivered_utc"):
        bv = wit.get(req)
        rv = wit_rec.get(req)
        if bv in (None, ""):
            issues.append({"code": "PROVENANCE_WITNESS_FIELD_NULL", "detail": f"{op_id}: witness binding {req}"})
        elif rv not in (None, "") and bv != rv:
            issues.append(
                {
                    "code": "PROVENANCE_WITNESS_BINDING_MISMATCH",
                    "detail": f"{op_id}: witness binding {req} != artifact",
                }
            )
    return issues


def _workload_phase_receipt_ok(exp: Any, receipts: list[ExecutionReceipt]) -> bool:
    phase = str(exp.phase or "")
    if not phase.startswith("P"):
        return True
    wphase = f"W{phase[1:]}"
    for r in receipts:
        if r.phase not in (wphase, phase) and r.phase_norm != phase:
            continue
        if r.receipt_type == "sql_commit" and int(r.raw.get("sql_exit", 0)) != 0:
            continue
        if r.receipt_type in ("sql_commit", "phase_boundary", "key_anchor", "barrier"):
            return True
    return False


@dataclass
class ProvenanceEvidence:
    doc: dict[str, Any]
    errors: list[str] = field(default_factory=list)


def load_provenance_evidence(path: str) -> ProvenanceEvidence:
    errors: list[str] = []
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    if doc.get("contract_version") != PROVENANCE_EVIDENCE_VERSION:
        errors.append(f"provenance contract_version != {PROVENANCE_EVIDENCE_VERSION}")
    if str(doc.get("run_id", "")) != RUN_ID:
        errors.append("provenance run_id mismatch")
    if doc.get("evidence_class") not in (PRIMARY_CLASS, COMPONENT_CLASS):
        errors.append("provenance evidence_class missing or invalid")
    bindings = doc.get("event_bindings")
    if not isinstance(bindings, list) or not bindings:
        errors.append("provenance event_bindings[] required")
    return ProvenanceEvidence(doc=doc, errors=errors)


def _envelope_source(value_raw: str) -> dict[str, Any] | None:
    try:
        val = json.loads(value_raw)
    except json.JSONDecodeError:
        return None
    if isinstance(val, dict) and isinstance(val.get("payload"), dict):
        val = val["payload"]
    src = val.get("source") if isinstance(val, dict) else None
    return src if isinstance(src, dict) else None


def _scn_value(obj: Any) -> int | None:
    if isinstance(obj, dict):
        v = obj.get("value", obj.get("scn"))
    else:
        v = obj
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _kafka_ts_utc(row: dict[str, Any]) -> datetime | None:
    ts = row.get("timestamp")
    if ts is None:
        return None
    try:
        ms = int(ts)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def verify_provenance_crossbind(
    evidence: ProvenanceEvidence,
    *,
    oracle: ExpectedOracle,
    business_ops: list[ParsedBusinessOp],
    kafka_rows: list[dict[str, Any]],
    lane: str,
    acceptance_mode: str,
    measurement_consumer_start_utc: str | None = None,
    execution_receipts: list[ExecutionReceipt] | None = None,
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []

    for e in evidence.errors:
        issues.append({"code": "PROVENANCE_EVIDENCE_INVALID", "detail": e})
    if issues:
        return issues

    doc = evidence.doc
    cls = doc.get("evidence_class")
    if acceptance_mode == "live" and cls != PRIMARY_CLASS:
        issues.append(
            {
                "code": "PROVENANCE_NOT_PRIMARY",
                "detail": "live acceptance requires evidence_class primary_stock37",
            }
        )
    expected_ops = list(oracle.committed_kafka)
    expected_ids = {op.op_id for op in expected_ops}
    bindings = doc.get("event_bindings") or []

    bind_by_id: dict[str, dict] = {}
    for i, b in enumerate(bindings):
        if not isinstance(b, dict):
            issues.append({"code": "PROVENANCE_BINDING_INVALID", "detail": f"bindings[{i}] not object"})
            continue
        oid = b.get("op_id")
        if not oid:
            issues.append({"code": "PROVENANCE_BINDING_INVALID", "detail": f"bindings[{i}] missing op_id"})
            continue
        if oid in bind_by_id:
            issues.append({"code": "PROVENANCE_BINDING_DUPLICATE", "detail": f"duplicate op_id {oid}"})
        bind_by_id[str(oid)] = b

    missing = sorted(expected_ids - set(bind_by_id))
    extra = sorted(set(bind_by_id) - expected_ids)
    if missing:
        issues.append(
            {
                "code": "PROVENANCE_BINDING_INCOMPLETE",
                "detail": f"missing bindings for {len(missing)} op_ids",
            }
        )
    if extra:
        issues.append({"code": "PROVENANCE_BINDING_EXTRA", "detail": f"unexpected op_ids {extra[:6]}"})

    op_by_id = {op.op_id: op for op in expected_ops}
    coord_map: dict[tuple[str, int, int], str] = {}

    row_index: dict[tuple[str, int, int], dict] = {}
    for r in kafka_rows:
        coord = (str(r["topic"]), int(r["partition"]), int(r["kafka_offset"]))
        row_index[coord] = r

    parsed_by_coord: dict[tuple[str, int, int], ParsedBusinessOp] = {}
    for op in business_ops:
        parsed_by_coord[(op.topic, op.partition, op.kafka_offset)] = op

    consumer_start: datetime | None = None
    if measurement_consumer_start_utc:
        try:
            consumer_start = parse_utc_strict(measurement_consumer_start_utc, field="consumer_start_utc")
        except ValueError as e:
            issues.append({"code": "PROVENANCE_TEMPORAL_INCOMPLETE", "detail": str(e)})

    for op_id, b in bind_by_id.items():
        if op_id not in op_by_id:
            continue
        exp = op_by_id[op_id]
        if b.get("rollback_binding"):
            issues.append(
                {"code": "PROVENANCE_ROLLBACK_BINDING", "detail": f"{op_id}: forbidden rollback binding"}
            )
            continue

        coord = b.get("kafka_coord")
        if not isinstance(coord, dict):
            issues.append({"code": "PROVENANCE_KAFKA_COORD_MISSING", "detail": f"{op_id}: kafka_coord required"})
            continue
        try:
            tp = (str(coord["topic"]), int(coord["partition"]), int(coord["offset"]))
        except (KeyError, TypeError, ValueError):
            issues.append({"code": "PROVENANCE_KAFKA_COORD_INVALID", "detail": f"{op_id}: bad kafka_coord"})
            continue
        if tp in coord_map:
            issues.append(
                {
                    "code": "PROVENANCE_KAFKA_COORD_COLLISION",
                    "detail": f"{op_id} shares coord with {coord_map[tp]}",
                }
            )
        coord_map[tp] = op_id

        parsed = parsed_by_coord.get(tp)
        if parsed is None:
            issues.append({"code": "PROVENANCE_KAFKA_ROW_MISSING", "detail": f"{op_id}: no parsed op at {tp}"})
        elif not ops_match(exp, parsed):
            issues.append(
                {
                    "code": "PROVENANCE_EXPECTED_OP_MISMATCH",
                    "detail": f"{op_id}: kafka row at coord != expected op identity",
                }
            )

        row = row_index.get(tp)
        if not row:
            issues.append({"code": "PROVENANCE_KAFKA_ROW_MISSING", "detail": f"{op_id}: raw row missing"})
            continue
        src = _envelope_source(row.get("value_raw") or "")
        if not src:
            issues.append({"code": "PROVENANCE_KAFKA_SOURCE_MISSING", "detail": f"{op_id}: envelope source"})
            continue

        ks = b.get("kafka_source")
        if not isinstance(ks, dict) or not ks:
            issues.append({"code": "PROVENANCE_KAFKA_SOURCE_EMPTY", "detail": f"{op_id}: kafka_source empty"})
            continue

        env_tx = str(src.get("txId", ""))
        bind_tx = str(ks.get("txId", ""))
        if env_tx != bind_tx:
            issues.append({"code": "PROVENANCE_TXID_MISMATCH", "detail": f"{op_id}: binding kafka_source txId"})
        if parse_stock37_tx_id(env_tx) is None:
            issues.append({"code": "PROVENANCE_TXID_FORMAT", "detail": f"{op_id}: envelope txId not stock37"})

        env_scn = _scn_value(src.get("scn"))
        bind_scn = _scn_value(ks.get("scn"))
        if env_scn is None or bind_scn is None or env_scn != bind_scn:
            issues.append({"code": "PROVENANCE_SCN_MISMATCH", "detail": f"{op_id}: kafka scn"})
        if str(src.get("schema", "")) != REQUIRED_SOURCE_SCHEMA:
            issues.append({"code": "PROVENANCE_SCHEMA_MISMATCH", "detail": f"{op_id}: envelope schema"})
        if str(src.get("table", "")).upper() != exp.table.upper():
            issues.append({"code": "PROVENANCE_TABLE_MISMATCH", "detail": f"{op_id}: envelope table"})

        ora = b.get("oracle")
        if not isinstance(ora, dict):
            issues.append({"code": "PROVENANCE_ORACLE_MISSING", "detail": f"{op_id}: oracle block"})
            continue

        commit_ref = ora.get("commit_evidence_ref")
        if not isinstance(commit_ref, dict):
            issues.append({"code": "PROVENANCE_COMMIT_EVIDENCE_MISSING", "detail": f"{op_id}: commit_evidence_ref"})
            continue
        commit_rec, _sha_c, err_c = resolve_artifact_record(commit_ref)
        if err_c:
            issues.append({"code": "PROVENANCE_ORACLE_ARTIFACT", "detail": f"{op_id}: {err_c}"})
            continue
        assert commit_rec is not None

        wit = b.get("srl_witness")
        if not isinstance(wit, dict) or not wit:
            issues.append({"code": "PROVENANCE_WITNESS_EMPTY", "detail": f"{op_id}: srl_witness empty"})
            continue
        wit_ref = wit.get("artifact_ref")
        if not isinstance(wit_ref, dict):
            issues.append({"code": "PROVENANCE_ARTIFACT_REF_MISSING", "detail": f"{op_id}: witness artifact_ref"})
            continue
        wit_rec, _sha_w, err_w = resolve_artifact_record(wit_ref)
        if err_w:
            issues.append({"code": "PROVENANCE_WITNESS_ARTIFACT", "detail": f"{op_id}: {err_w}"})
            continue
        assert wit_rec is not None

        issues.extend(_compare_commit_identity_fields(ora, commit_rec, exp, parsed, lane, op_id))

        ok_xid, xid_err = verify_oracle_xid_binding(ora, commit_rec, env_tx)
        if not ok_xid:
            code = (
                "PROVENANCE_XID_RAW_INCONSISTENT"
                if xid_err and "contradict" in xid_err
                else "PROVENANCE_XID_MISMATCH"
            )
            issues.append({"code": code, "detail": f"{op_id}: {xid_err}"})

        if str(commit_rec.get("run_id", "")) != RUN_ID:
            issues.append({"code": "PROVENANCE_ORACLE_ARTIFACT", "detail": f"{op_id}: commit run_id"})
        if str(wit_rec.get("run_id", "")) != RUN_ID:
            issues.append({"code": "PROVENANCE_WITNESS_ARTIFACT", "detail": f"{op_id}: witness run_id"})
        if str(commit_rec.get("op_id", "")) != op_id or str(wit_rec.get("op_id", "")) != op_id:
            issues.append({"code": "PROVENANCE_BINDING_OP_MISMATCH", "detail": f"{op_id}: artifact op_id"})

        inst_a = commit_rec.get("instance_number")
        thr_a = commit_rec.get("thread_number")
        if inst_a is None or thr_a is None:
            issues.append({"code": "PROVENANCE_ORACLE_FIELD_NULL", "detail": f"{op_id}: instance/thread in commit artifact"})
        else:
            for label, block, field in (
                ("oracle", ora, "instance_number"),
                ("oracle", ora, "thread_number"),
                ("witness", wit, "thread"),
                ("witness_rec", wit_rec, "thread"),
            ):
                val = block.get(field)
                if field == "thread" and label.startswith("witness"):
                    if val is not None and int(val) != int(thr_a):
                        issues.append(
                            {
                                "code": "PROVENANCE_WITNESS_IDENTITY",
                                "detail": f"{op_id}: {label}.{field} != commit artifact thread",
                            }
                        )
                elif field == "thread_number" and val is not None and int(val) != int(thr_a):
                    issues.append(
                        {
                            "code": "PROVENANCE_ORACLE_FIELD_MISMATCH",
                            "detail": f"{op_id}: oracle.thread_number != commit artifact",
                        }
                    )
                elif field == "instance_number" and val is not None and int(val) != int(inst_a):
                    issues.append(
                        {
                            "code": "PROVENANCE_ORACLE_FIELD_MISMATCH",
                            "detail": f"{op_id}: oracle.instance_number != commit artifact",
                        }
                    )

        issues.extend(_compare_witness_binding(wit, wit_rec, op_id))

        if str(wit_rec.get("lane", "")) != lane:
            issues.append({"code": "PROVENANCE_WITNESS_IDENTITY", "detail": f"{op_id}: witness artifact lane"})

        outcome_a = str(commit_rec.get("commit_outcome", "")).lower()
        if outcome_a != "success":
            issues.append({"code": "PROVENANCE_COMMIT_NOT_SUCCESS", "detail": f"{op_id}: artifact commit_outcome"})
        elif str(ora.get("commit_outcome", "")).lower() not in ("", "success") and str(ora.get("commit_outcome", "")).lower() != outcome_a:
            issues.append({"code": "PROVENANCE_COMMIT_NOT_SUCCESS", "detail": f"{op_id}: binding commit_outcome != artifact"})

        ck_val = _scn_value(ora.get("checkpoint_scn"))
        ck_src = ora.get("checkpoint_scn", {}).get("source") if isinstance(ora.get("checkpoint_scn"), dict) else "kafka_envelope"
        if ck_val is None:
            issues.append({"code": "PROVENANCE_CHECKPOINT_SCN", "detail": f"{op_id}: checkpoint_scn missing"})
        elif str(ck_src) not in CHECKPOINT_SCN_SOURCES:
            issues.append({"code": "PROVENANCE_CHECKPOINT_SOURCE", "detail": f"{op_id}: checkpoint source {ck_src!r}"})
        elif env_scn is not None and ck_val != env_scn:
            issues.append(
                {"code": "PROVENANCE_CHECKPOINT_SCN", "detail": f"{op_id}: checkpoint != envelope scn"}
            )

        cm_artifact = commit_rec.get("commit_scn")
        cm_src = commit_rec.get("commit_scn_source")
        cm_val = _scn_value(cm_artifact)
        if cm_val is None:
            issues.append({"code": "PROVENANCE_COMMIT_SCN", "detail": f"{op_id}: commit_scn missing in artifact"})
        elif str(cm_src) not in ORACLE_COMMIT_SCN_SOURCES:
            issues.append(
                {
                    "code": "PROVENANCE_COMMIT_SCN_SOURCE",
                    "detail": f"{op_id}: commit_scn_source {cm_src!r} not independent redo/end",
                }
            )
        else:
            bind_cm = ora.get("commit_scn")
            bind_cm_val = _scn_value(bind_cm)
            if bind_cm_val is not None and bind_cm_val != cm_val:
                issues.append(
                    {"code": "PROVENANCE_COMMIT_SCN", "detail": f"{op_id}: binding commit_scn != artifact"}
                )

        growth = wit_rec.get("growth_state") or wit.get("growth_state")
        if growth not in GROWTH_STATES:
            issues.append({"code": "PROVENANCE_GROWTH_STATE", "detail": f"{op_id}: invalid growth_state"})
        elif wit.get("growth_state") and wit.get("growth_state") != growth:
            issues.append({"code": "PROVENANCE_GROWTH_STATE", "detail": f"{op_id}: binding growth != artifact"})

        for req in ("incarnation", "thread", "sequence", "member", "sampled_utc", "archive_delivered_utc"):
            if wit_rec.get(req) in (None, ""):
                issues.append({"code": "PROVENANCE_WITNESS_FIELD_NULL", "detail": f"{op_id}: witness artifact {req}"})

        try:
            sampled = parse_utc_strict(str(wit_rec.get("sampled_utc", "")), field="sampled_utc")
            archive_del = parse_utc_strict(
                str(wit_rec.get("archive_delivered_utc", "")), field="archive_delivered_utc"
            )
        except ValueError as e:
            issues.append({"code": "PROVENANCE_TEMPORAL_INCOMPLETE", "detail": f"{op_id}: {e}"})
            continue

        cap_ts = _kafka_ts_utc(row)
        if cap_ts is None:
            issues.append({"code": "PROVENANCE_TEMPORAL_INCOMPLETE", "detail": f"{op_id}: kafka timestamp missing"})
        else:
            if consumer_start and sampled < consumer_start:
                issues.append(
                    {"code": "PROVENANCE_TEMPORAL_ORDER", "detail": f"{op_id}: sampled before consumer_start"}
                )
            if sampled > cap_ts:
                issues.append(
                    {"code": "PROVENANCE_TEMPORAL_ORDER", "detail": f"{op_id}: sampled after kafka capture ts"}
                )

        if archive_del < sampled:
            issues.append(
                {"code": "PROVENANCE_TEMPORAL_ORDER", "detail": f"{op_id}: archive before witness sample"}
            )

        if b.get("uses_row_inst_as_instance"):
            issues.append(
                {"code": "PROVENANCE_INST_FORBIDDEN", "detail": f"{op_id}: must not bind INST as instance_number"}
            )

        if execution_receipts is not None and not _workload_phase_receipt_ok(exp, execution_receipts):
            issues.append(
                {
                    "code": "PROVENANCE_EXECUTION_RECEIPT_MISSING",
                    "detail": f"{op_id}: no sql_commit receipt for phase {exp.phase}",
                }
            )

        if exp.seq_on_key is not None and parsed is not None:
            same_key = [
                o
                for o in expected_ops
                if o.table == exp.table
                and o.key == exp.key
                and o.op_id in bind_by_id
                and bind_by_id[o.op_id].get("kafka_coord")
            ]
            ordered: list[str] = []
            for o in same_key:
                bc = bind_by_id[o.op_id].get("kafka_coord") or {}
                try:
                    tp_o = (str(bc["topic"]), int(bc["partition"]), int(bc["offset"]))
                except (KeyError, TypeError, ValueError):
                    continue
                ordered.append((tp_o, o.op_id))
            ordered.sort(key=lambda x: x[0])
            seq_actual = next((i + 1 for i, (_, oid) in enumerate(ordered) if oid == op_id), None)
            if seq_actual is not None and int(exp.seq_on_key) != seq_actual:
                issues.append(
                    {
                        "code": "PROVENANCE_SEQ_ON_KEY_MISMATCH",
                        "detail": f"{op_id}: seq_on_key {exp.seq_on_key} != binding order {seq_actual}",
                    }
                )

    if acceptance_mode == "live" and cls == PRIMARY_CLASS and not issues:
        if len(bind_by_id) != len(expected_ids):
            issues.append(
                {
                    "code": "PROVENANCE_BINDING_INCOMPLETE",
                    "detail": f"live requires {len(expected_ids)} bindings",
                }
            )

    return issues


PROVENANCE_INPUT_INCOMPLETE_CODES = frozenset(
    {
        "PROVENANCE_EVIDENCE_INVALID",
        "PROVENANCE_EVIDENCE_MISSING",
        "PROVENANCE_NOT_PRIMARY",
        "PROVENANCE_ORACLE_FIELD_MISMATCH",
        "PROVENANCE_COMMIT_RECORD_INCOMPLETE",
        "PROVENANCE_COMMIT_RECORD_MISMATCH",
        "PROVENANCE_WITNESS_BINDING_MISMATCH",
        "PROVENANCE_XID_RAW_INCONSISTENT",
        "PROVENANCE_EXECUTION_RECEIPT_MISSING",
        "PROVENANCE_BINDING_INCOMPLETE",
        "PROVENANCE_BINDING_DUPLICATE",
        "PROVENANCE_BINDING_EXTRA",
        "PROVENANCE_BINDING_INVALID",
        "PROVENANCE_BINDING_OP_MISMATCH",
        "PROVENANCE_WITNESS_EMPTY",
        "PROVENANCE_ORACLE_FIELD_NULL",
        "PROVENANCE_ARTIFACT_REF_MISSING",
        "PROVENANCE_COMMIT_EVIDENCE_MISSING",
        "PROVENANCE_ORACLE_ARTIFACT",
        "PROVENANCE_WITNESS_ARTIFACT",
        "PROVENANCE_RECEIPT_PARSE",
        "PROVENANCE_CROSSBIND_DEFERRED",
        "PROVENANCE_KAFKA_COORD_MISSING",
        "PROVENANCE_ORACLE_MISSING",
        "PROVENANCE_KAFKA_SOURCE_EMPTY",
        "PROVENANCE_WITNESS_FIELD_NULL",
        "PROVENANCE_TEMPORAL_INCOMPLETE",
        "PROVENANCE_KAFKA_COORD_INVALID",
        "PROVENANCE_ROLLBACK_BINDING",
        "PROVENANCE_TXID_MISMATCH",
        "PROVENANCE_TXID_FORMAT",
        "PROVENANCE_XID_MISMATCH",
        "PROVENANCE_SCN_MISMATCH",
        "PROVENANCE_SCHEMA_MISMATCH",
        "PROVENANCE_TABLE_MISMATCH",
        "PROVENANCE_WITNESS_IDENTITY",
        "PROVENANCE_COMMIT_NOT_SUCCESS",
        "PROVENANCE_COMMIT_SCN",
        "PROVENANCE_COMMIT_SCN_SOURCE",
        "PROVENANCE_CHECKPOINT_SCN",
        "PROVENANCE_CHECKPOINT_SOURCE",
        "PROVENANCE_GROWTH_STATE",
        "PROVENANCE_TEMPORAL_ORDER",
        "PROVENANCE_KAFKA_ROW_MISSING",
        "PROVENANCE_KAFKA_COORD_COLLISION",
    }
)


def verify_provenance_for_acceptance(
    evidence: ProvenanceEvidence,
    *,
    acceptance_mode: str,
    oracle: ExpectedOracle | None = None,
    business_ops: list[ParsedBusinessOp] | None = None,
    kafka_rows: list[dict[str, Any]] | None = None,
    lane: str = "baseline",
    measurement_consumer_start_utc: str | None = None,
    execution_receipts: list[ExecutionReceipt] | None = None,
) -> list[dict[str, str]]:
    if oracle is not None and business_ops is not None and kafka_rows is not None:
        return verify_provenance_crossbind(
            evidence,
            oracle=oracle,
            business_ops=business_ops,
            kafka_rows=kafka_rows,
            lane=lane,
            acceptance_mode=acceptance_mode,
            measurement_consumer_start_utc=measurement_consumer_start_utc,
            execution_receipts=execution_receipts,
        )
    issues: list[dict[str, str]] = []
    for e in evidence.errors:
        issues.append({"code": "PROVENANCE_EVIDENCE_INVALID", "detail": e})
    if acceptance_mode == "live":
        issues.append(
            {"code": "PROVENANCE_CROSSBIND_DEFERRED", "detail": "kafka context required for live provenance"}
        )
    return issues
