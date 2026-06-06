"""Feast entity definitions."""

from feast import Entity, ValueType

patient = Entity(
    name="patient",
    join_keys=["patient_id"],
    value_type=ValueType.STRING,
    description="A healthcare patient identified by UUID.",
)

provider = Entity(
    name="provider",
    join_keys=["provider_id"],
    value_type=ValueType.STRING,
    description="A healthcare provider (physician/NP/PA) identified by UUID.",
)
