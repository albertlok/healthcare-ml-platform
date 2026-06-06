"""
Feast feature service definitions.

A FeatureService bundles feature views into a named, versioned artifact
consumed by model training and inference. Versioning allows safe model
upgrades without breaking existing consumers.
"""

from feast import FeatureService

from feature_views import (
    appointment_context,
    patient_appointment_stats,
    patient_demographics,
    provider_stats,
)

# Used during model training (offline retrieval, point-in-time correct)
no_show_training_fs = FeatureService(
    name="no_show_training_v1",
    features=[
        patient_appointment_stats[
            ["no_show_rate_30d", "no_show_rate_90d", "cancellation_rate_90d",
             "total_appointments_90d", "avg_lead_time_days", "last_appointment_days_ago"]
        ],
        patient_demographics[
            ["age_at_appointment", "gender_encoded", "insurance_type_encoded",
             "distance_to_clinic_miles", "preferred_communication_encoded", "has_chronic_condition"]
        ],
        provider_stats[
            ["provider_no_show_rate_90d"]
        ],
    ],
    description="Full feature set for training the no-show classifier v1.",
    tags={"model": "no_show_classifier", "version": "1"},
)

# Used during real-time inference (online retrieval, low latency)
no_show_inference_fs = FeatureService(
    name="no_show_inference_v1",
    features=[
        patient_appointment_stats[
            ["no_show_rate_30d", "no_show_rate_90d", "total_appointments_90d",
             "avg_lead_time_days", "last_appointment_days_ago"]
        ],
        patient_demographics[
            ["age_at_appointment", "insurance_type_encoded",
             "distance_to_clinic_miles", "has_chronic_condition"]
        ],
        appointment_context[
            ["appointment_type_encoded", "day_of_week", "hour_of_day",
             "is_reminder_sent", "lead_time_hours"]
        ],
        provider_stats[["provider_no_show_rate_90d"]],
    ],
    description="Online feature bundle for real-time no-show inference.",
    tags={"model": "no_show_classifier", "version": "1", "mode": "online"},
)
