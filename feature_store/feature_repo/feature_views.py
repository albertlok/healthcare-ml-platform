"""
Feast feature view definitions.

Three feature views covering the full signal set for the no-show classifier:
  1. patient_appointment_stats   — rolling behavioral history
  2. patient_demographics        — static patient attributes
  3. appointment_context         — appointment-level features
  4. provider_stats              — provider-level historical rate

Point-in-time correct joins are handled automatically by Feast when calling
store.get_historical_features() with a timestamped entity_df.
"""

from datetime import timedelta

from data_sources import (
    appointment_context_source,
    patient_demographics_source,
    patient_stats_source,
    provider_stats_source,
)
from entities import patient, provider
from feast import FeatureView, Field
from feast.types import Bool, Float32, Int32

# ── 1. Patient Appointment Statistics ────────────────────────────────────────
patient_appointment_stats = FeatureView(
    name="patient_appointment_stats",
    entities=[patient],
    # ttl (time-to-live): how long a feature value stays valid after it was computed.
    # If a patient's last feature computation was >90 days ago, Feast returns NULL
    # instead of stale data. This prevents the model from using ancient statistics
    # for patients who haven't been seen recently. Set this to match your rolling window.
    ttl=timedelta(days=90),
    schema=[
        Field(
            name="no_show_rate_30d",
            dtype=Float32,
            description="No-show rate over the prior 30 days. Range [0.0, 1.0].",
        ),
        Field(
            name="no_show_rate_90d",
            dtype=Float32,
            description="No-show rate over the prior 90 days. Range [0.0, 1.0].",
        ),
        Field(
            name="cancellation_rate_90d",
            dtype=Float32,
            description="Cancellation rate over the prior 90 days.",
        ),
        Field(
            name="total_appointments_90d",
            dtype=Int32,
            description="Total appointment volume over 90 days.",
        ),
        Field(
            name="avg_lead_time_days",
            dtype=Float32,
            description="Average days between scheduling and appointment start.",
        ),
        Field(
            name="last_appointment_days_ago",
            dtype=Int32,
            description="Days since the patient's most recent completed appointment.",
        ),
    ],
    source=patient_stats_source,
    tags={"team": "ml-engineering", "domain": "appointments"},
    description="Rolling appointment behavioral statistics per patient.",
)

# ── 2. Patient Demographics ───────────────────────────────────────────────────
patient_demographics = FeatureView(
    name="patient_demographics",
    entities=[patient],
    ttl=timedelta(days=365),
    schema=[
        Field(
            name="age_at_appointment",
            dtype=Int32,
            description="Patient age in years at the time of the appointment.",
        ),
        Field(
            name="gender_encoded",
            dtype=Int32,
            description="Ordinal-encoded gender. 0=MALE, 1=FEMALE, 2=OTHER.",
        ),
        Field(
            name="insurance_type_encoded",
            dtype=Int32,
            description="Ordinal-encoded insurance type.",
        ),
        Field(
            name="distance_to_clinic_miles",
            dtype=Float32,
            description="Approximate driving distance to clinic.",
        ),
        Field(
            name="preferred_communication_encoded",
            dtype=Int32,
            description="Ordinal-encoded preferred communication channel.",
        ),
        Field(
            name="has_chronic_condition",
            dtype=Bool,
            description="Whether patient has a chronic condition on record.",
        ),
    ],
    source=patient_demographics_source,
    tags={"team": "ml-engineering", "domain": "patients"},
    description="Patient demographic and insurance attributes.",
)

# ── 3. Appointment Context ────────────────────────────────────────────────────
appointment_context = FeatureView(
    name="appointment_context",
    entities=[patient],
    ttl=timedelta(days=1),
    schema=[
        Field(
            name="appointment_type_encoded",
            dtype=Int32,
            description="Ordinal-encoded appointment type.",
        ),
        Field(
            name="day_of_week",
            dtype=Int32,
            description="Day of week for scheduled appointment. 1=Sunday, 7=Saturday.",
        ),
        Field(
            name="hour_of_day",
            dtype=Int32,
            description="Hour of day (0-23) for scheduled appointment.",
        ),
        Field(
            name="scheduled_month",
            dtype=Int32,
            description="Calendar month of scheduled appointment.",
        ),
        Field(
            name="is_morning_appointment",
            dtype=Bool,
            description="True if appointment is between 08:00 and 12:00.",
        ),
        Field(
            name="is_reminder_sent",
            dtype=Bool,
            description="Whether an automated reminder was sent to the patient.",
        ),
        Field(
            name="lead_time_hours",
            dtype=Int32,
            description="Hours between scheduling and appointment start.",
        ),
    ],
    source=appointment_context_source,
    tags={"team": "ml-engineering", "domain": "appointments"},
    description="Appointment-level contextual features for real-time inference.",
)

# ── 4. Provider Statistics ────────────────────────────────────────────────────
provider_stats = FeatureView(
    name="provider_stats",
    entities=[provider],
    ttl=timedelta(days=90),
    schema=[
        Field(
            name="provider_no_show_rate_90d",
            dtype=Float32,
            description="Provider's historical no-show rate over 90 days.",
        ),
        Field(
            name="provider_total_appointments_90d",
            dtype=Int32,
            description="Provider's total appointment volume over 90 days.",
        ),
        Field(
            name="provider_avg_appointment_duration",
            dtype=Float32,
            description="Provider's average scheduled appointment duration in minutes.",
        ),
    ],
    source=provider_stats_source,
    tags={"team": "ml-engineering", "domain": "providers"},
    description="Provider-level appointment statistics.",
)
