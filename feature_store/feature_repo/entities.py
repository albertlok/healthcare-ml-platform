"""
Feast entity definitions.

An Entity in Feast is like a primary key concept — it's the thing features belong to.
When you call store.get_historical_features() or store.get_online_features(), Feast uses
the entity key(s) to look up which feature rows to return for each request.

Think of entities as "who or what we're making predictions about."
For this project: patients (who might no-show) and providers (whose schedule we protect).
"""

from feast import Entity, ValueType

# The patient entity tells Feast that features in patient_appointment_stats,
# patient_demographics, and appointment_context are keyed by patient_id.
# When you pass a list of patient_ids to get_online_features(), Feast does the
# equivalent of SELECT * FROM feature_view WHERE patient_id IN (...).
patient = Entity(
    name="patient",
    join_keys=["patient_id"],
    value_type=ValueType.STRING,
    description="A healthcare patient identified by UUID.",
)

# Separate entity for providers so we can compute provider-level features
# (e.g., provider_no_show_rate_90d) independently of patient features and
# join them together at training/inference time.
provider = Entity(
    name="provider",
    join_keys=["provider_id"],
    value_type=ValueType.STRING,
    description="A healthcare provider (physician/NP/PA) identified by UUID.",
)
