#!/usr/bin/env python3
"""Test CONTACT_TYPENAMES mapping consistency.
Run after pip install --upgrade meshcore to catch drift.
"""
from meshcore.meshcore_parser import CONTACT_TYPENAMES

# Expected mapping: index → (short name, Polish label in WebUI)
EXPECTED = {
    0: ("NONE",   "NONE"),
    1: ("CLI",    "Klient"),
    2: ("REP",    "Repeater"),
    3: ("ROOM",   "Room"),
    4: ("SENS",   "Sensor"),
}

errors = []
for i, (short, label) in EXPECTED.items():
    if i >= len(CONTACT_TYPENAMES):
        errors.append(f"Missing index {i}: package has only {len(CONTACT_TYPENAMES)} entries")
    elif CONTACT_TYPENAMES[i] != short:
        errors.append(f"Index {i}: package={CONTACT_TYPENAMES[i]}, expected={short}")

if errors:
    print(f"❌ FAILED ({len(errors)} errors):")
    for e in errors:
        print(f"   {e}")
    exit(1)

print(f"✅ All {len(EXPECTED)} entries match meshcore v2.3.7")
for i, (short, label) in EXPECTED.items():
    print(f"   [{i}] {short:6s} → {label}")
