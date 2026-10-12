"""Oracle XID ↔ stock37 Debezium source.txId (USN/SLOT/SQN)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# Debezium stock37: 0xUUUU.SSS.QQQQQQQQ (hex USN, slot, sqn)
_STOCK37_TXID = re.compile(
    r"^0x([0-9a-fA-F]{1,4})\.([0-9a-fA-F]{1,4})\.([0-9a-fA-F]{1,8})$"
)
_RAW8_HEX = re.compile(r"^[0-9a-fA-F]{16}$")

_USN_BITS = 16
_SLOT_BITS = 16
_SQN_BITS = 32


@dataclass(frozen=True)
class OracleXid:
    usn: int
    slot: int
    sqn: int
    raw_hex: str | None = None

    def stock37_tx_id(self) -> str:
        return f"0x{self.usn:04x}.{self.slot:04x}.{self.sqn:08x}"


def _strict_uint(value: Any, *, bits: int, field: str) -> tuple[int | None, str | None]:
    if isinstance(value, bool):
        return None, f"{field} bool forbidden"
    if isinstance(value, float):
        return None, f"{field} fraction forbidden"
    if isinstance(value, int):
        v = value
    elif isinstance(value, str):
        s = value.strip()
        if not s or not s.isdigit():
            return None, f"{field} not integer literal"
        v = int(s)
    else:
        return None, f"{field} bad type"
    if v < 0 or v >= (1 << bits):
        return None, f"{field} out of range for {bits}-bit"
    return v, None


def parse_stock37_tx_id(tx_id: str) -> OracleXid | None:
    m = _STOCK37_TXID.match(str(tx_id).strip())
    if not m:
        return None
    usn = int(m.group(1), 16)
    slot = int(m.group(2), 16)
    sqn = int(m.group(3), 16)
    if usn >= (1 << _USN_BITS) or slot >= (1 << _SLOT_BITS) or sqn >= (1 << _SQN_BITS):
        return None
    return OracleXid(usn=usn, slot=slot, sqn=sqn)


def xid_triplet_from_raw8(raw16: str) -> OracleXid | None:
    s = raw16.lower()
    if not _RAW8_HEX.match(s):
        return None
    usn = int(s[0:4], 16)
    slot = int(s[4:8], 16)
    sqn = int(s[8:16], 16)
    return OracleXid(usn=usn, slot=slot, sqn=sqn, raw_hex=s)


def _normalize_raw8_field(value: Any, field: str) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    s = str(value).strip().lower()
    if not _RAW8_HEX.match(s):
        return None, f"{field} not 16 hex digits"
    return s, None


def oracle_xid_internal_consistent(ora: dict[str, Any]) -> tuple[bool, str | None]:
    """Every provided encoding must agree; parse all supplied forms."""
    if not isinstance(ora, dict):
        return False, "oracle block not object"

    has_triplet = all(k in ora for k in ("xidusn", "xidslot", "xidsqn"))
    raw_bytes, err_b = _normalize_raw8_field(ora.get("raw_xid_bytes_hex"), "raw_xid_bytes_hex")
    if err_b:
        return False, err_b
    raw_alias, err_a = _normalize_raw8_field(ora.get("raw_xid_hex"), "raw_xid_hex")
    if err_a:
        return False, err_a
    if raw_bytes is not None and raw_alias is not None and raw_bytes != raw_alias:
        return False, "raw_xid_bytes_hex contradicts raw_xid_hex"

    triplet: OracleXid | None = None
    if has_triplet:
        usn, e1 = _strict_uint(ora.get("xidusn"), bits=_USN_BITS, field="xidusn")
        slot, e2 = _strict_uint(ora.get("xidslot"), bits=_SLOT_BITS, field="xidslot")
        sqn, e3 = _strict_uint(ora.get("xidsqn"), bits=_SQN_BITS, field="xidsqn")
        for e in (e1, e2, e3):
            if e:
                return False, e
        assert usn is not None and slot is not None and sqn is not None
        triplet = OracleXid(usn=usn, slot=slot, sqn=sqn)

    raw_effective = raw_bytes or raw_alias
    from_raw: OracleXid | None = None
    if raw_effective is not None:
        from_raw = xid_triplet_from_raw8(raw_effective)
        if from_raw is None:
            return False, "raw_xid not parseable"

    if triplet is not None and from_raw is not None and not xids_equal(triplet, from_raw):
        return False, "raw_xid_bytes_hex contradicts xid triplet"
    if triplet is None and from_raw is None:
        return False, "xidusn/xidslot/xidsqn or raw_xid_bytes_hex required"
    return True, None


def xid_from_oracle_fields(ora: dict[str, Any]) -> OracleXid | None:
    ok, _ = oracle_xid_internal_consistent(ora)
    if not ok:
        return None
    has_triplet = all(k in ora for k in ("xidusn", "xidslot", "xidsqn"))
    raw_bytes, _ = _normalize_raw8_field(ora.get("raw_xid_bytes_hex"), "raw_xid_bytes_hex")
    raw_alias, _ = _normalize_raw8_field(ora.get("raw_xid_hex"), "raw_xid_hex")
    raw_effective = raw_bytes or raw_alias
    if has_triplet:
        usn, _ = _strict_uint(ora["xidusn"], bits=_USN_BITS, field="xidusn")
        slot, _ = _strict_uint(ora["xidslot"], bits=_SLOT_BITS, field="xidslot")
        sqn, _ = _strict_uint(ora["xidsqn"], bits=_SQN_BITS, field="xidsqn")
        if usn is None or slot is None or sqn is None:
            return None
        return OracleXid(
            usn=usn,
            slot=slot,
            sqn=sqn,
            raw_hex=raw_effective,
        )
    if raw_effective:
        return xid_triplet_from_raw8(raw_effective)
    return None


def xid_from_raw8_hex(raw16: str) -> OracleXid | None:
    return xid_triplet_from_raw8(raw16)


def normalize_oracle_xid_legacy(raw: str) -> OracleXid | None:
    """Deprecated two-part hex; only when no stock37 txId available."""
    s = str(raw).strip()
    if "." not in s:
        return xid_from_raw8_hex(s) if _RAW8_HEX.match(s) else None
    parts = s.split(".")
    if len(parts) != 2:
        return None
    try:
        usn = int(parts[0], 16)
    except ValueError:
        return None
    tail = parts[1]
    if len(tail) >= 8:
        slot = int(tail[0:4], 16) if len(tail) >= 4 else 0
        sqn = int(tail[4:12], 16) if len(tail) >= 12 else int(tail[4:], 16)
    else:
        slot, sqn = 0, 0
    return OracleXid(usn=usn, slot=slot, sqn=sqn)


def xids_equal(a: OracleXid, b: OracleXid) -> bool:
    return a.usn == b.usn and a.slot == b.slot and a.sqn == b.sqn


def oracle_xid_matches_tx_id(ora: dict[str, Any], tx_id: str) -> tuple[bool, str | None]:
    kafka = parse_stock37_tx_id(tx_id)
    if kafka is None:
        return False, "kafka txId not stock37 0xUSN.SLOT.SQN"
    ok_int, int_err = oracle_xid_internal_consistent(ora)
    if not ok_int:
        return False, int_err
    ox = xid_from_oracle_fields(ora)
    if ox is None and ora.get("raw_xid"):
        ox = normalize_oracle_xid_legacy(str(ora["raw_xid"]))
    if ox is None:
        return False, "oracle xid not parseable (need xidusn/xidslot/xidsqn or raw_xid_bytes_hex)"
    if not xids_equal(ox, kafka):
        return False, f"xid {ox.stock37_tx_id()} != kafka {kafka.stock37_tx_id()}"
    return True, None


def verify_oracle_xid_binding(
    ora: dict[str, Any], commit_rec: dict[str, Any], env_tx: str
) -> tuple[bool, str | None]:
    """Binding oracle block must carry identity; artifact fields compared field-wise."""
    for field in ("xidusn", "xidslot", "xidsqn"):
        if ora.get(field) is None:
            return False, f"binding oracle missing {field}"
    ok_c, c_err = oracle_xid_internal_consistent(commit_rec)
    if not ok_c:
        return False, f"commit artifact xid: {c_err}"
    ok_int, int_err = oracle_xid_internal_consistent(ora)
    if not ok_int:
        return False, int_err
    for field in ("xidusn", "xidslot", "xidsqn", "raw_xid_bytes_hex", "raw_xid_hex"):
        if field not in commit_rec and field == "raw_xid_hex" and "raw_xid_bytes_hex" in commit_rec:
            continue
        if field not in commit_rec:
            continue
        ov = ora.get(field)
        cv = commit_rec.get(field)
        if ov is None and field.startswith("raw"):
            if field in ora or (field == "raw_xid_bytes_hex" and "raw_xid_hex" in ora):
                return False, f"binding oracle missing {field}"
            continue
        if ov is None:
            return False, f"binding oracle missing {field} (artifact has value)"
        if field.startswith("raw"):
            if str(ov).lower() != str(cv).lower():
                return False, "binding raw_xid != commit artifact"
        else:
            ou, e1 = _strict_uint(ov, bits=_USN_BITS if field == "xidusn" else _SLOT_BITS if field == "xidslot" else _SQN_BITS, field=field)
            cu, e2 = _strict_uint(cv, bits=_USN_BITS if field == "xidusn" else _SLOT_BITS if field == "xidslot" else _SQN_BITS, field=field)
            if e1 or e2 or ou != cu:
                return False, f"binding {field} != commit artifact"
    return oracle_xid_matches_tx_id(ora, env_tx)
