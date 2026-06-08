"""
Seed script: generate synthetic bronze Delta tables in MinIO.

Writes realistic appointment and patient records directly to
s3://healthcare/bronze/appointments_raw and patients_raw as Delta tables,
bypassing the Kafka → delta_sink path for local dev / CI use.

Run inside airflow-worker:
    python /opt/airflow/ingestion/seed_bronze.py --rows 2000
"""

from __future__ import annotations

import argparse
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
from deltalake import DeltaTable, write_deltalake

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

STORAGE_OPTIONS = {
    "endpoint_url": MINIO_ENDPOINT,
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "aws_allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
    "region": "us-east-1",
}

INSURANCE_TYPES = [
    "COMMERCIAL",
    "MEDICARE",
    "MEDICAID",
    "SELF_PAY",
    "WORKERS_COMP",
    "TRICARE",
    "UNKNOWN",
]
APPOINTMENT_TYPES = [
    "NEW_PATIENT",
    "FOLLOW_UP",
    "ANNUAL_WELLNESS",
    "URGENT_CARE",
    "TELEHEALTH",
    "SPECIALIST_REFERRAL",
    "PROCEDURE",
    "LAB_REVIEW",
]
EVENT_TYPES = ["SCHEDULED", "RESCHEDULED", "CANCELLED", "COMPLETED", "NO_SHOW"]
GENDERS = ["MALE", "FEMALE", "NON_BINARY", "PREFER_NOT_TO_SAY"]
COMM_CHANNELS = ["SMS", "EMAIL", "PHONE", "PORTAL"]
REG_SOURCES = ["WEB_PORTAL", "MOBILE_APP", "FRONT_DESK", "REFERRAL", "IMPORTED"]

PATIENT_POOL = [str(uuid.uuid4()) for _ in range(500)]
PROVIDER_POOL = [str(uuid.uuid4()) for _ in range(50)]
CLINIC_POOL = [str(uuid.uuid4()) for _ in range(10)]

NOW = datetime.now(timezone.utc)


def make_appointments(n: int) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        sched_offset = timedelta(days=random.randint(-180, 30), hours=random.randint(0, 23))
        scheduled_start = NOW + sched_offset
        event_type = random.choices(EVENT_TYPES, weights=[55, 10, 12, 18, 5])[0]
        rows.append(
            {
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "event_timestamp": int(NOW.timestamp() * 1000),
                "appointment_id": str(uuid.uuid4()),
                "patient_id": random.choice(PATIENT_POOL),
                "provider_id": random.choice(PROVIDER_POOL),
                "clinic_id": random.choice(CLINIC_POOL),
                "appointment_type": random.choice(APPOINTMENT_TYPES),
                "scheduled_start_ts": int(scheduled_start.timestamp() * 1000),
                "scheduled_duration_minutes": random.choice([15, 20, 30, 45, 60]),
                "is_reminder_sent": random.random() > 0.3,
                "insurance_type": random.choice(INSURANCE_TYPES),
                "lead_time_hours": random.randint(1, 720),
                "ingestion_date": NOW.date().isoformat(),
            }
        )
    return pd.DataFrame(rows)


def make_patients() -> pd.DataFrame:
    rows = []
    for patient_id in PATIENT_POOL:
        dob_offset = timedelta(days=random.randint(365 * 18, 365 * 85))
        dob = (NOW - dob_offset).date()
        rows.append(
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "REGISTERED",
                "event_timestamp": int(
                    (NOW - timedelta(days=random.randint(0, 730))).timestamp() * 1000
                ),
                "patient_id": patient_id,
                "date_of_birth": (dob - datetime(1970, 1, 1, tzinfo=timezone.utc).date()).days,
                "gender": random.choice(GENDERS),
                "zip_code": f"{random.randint(10000, 99999):05d}",
                "state": random.choice(["CA", "NY", "TX", "FL", "WA", "IL", "PA", "OH"]),
                "insurance_type": random.choice(INSURANCE_TYPES),
                "preferred_language": random.choices(
                    ["en", "es", "zh", "fr"], weights=[80, 10, 5, 5]
                )[0],
                "preferred_communication_channel": random.choice(COMM_CHANNELS),
                "distance_to_clinic_miles": round(random.uniform(0.5, 50.0), 2),
                "has_chronic_condition": random.random() > 0.7,
                "registration_source": random.choice(REG_SOURCES),
                "ingestion_date": NOW.date().isoformat(),
            }
        )
    return pd.DataFrame(rows)


def main(appointment_rows: int) -> None:
    print(
        f"Generating {appointment_rows} appointment events "
        f"and {len(PATIENT_POOL)} patient records..."
    )

    appt_df = make_appointments(appointment_rows)
    patient_df = make_patients()

    appt_path = "s3://healthcare/bronze/appointments_raw"
    patient_path = "s3://healthcare/bronze/patients_raw"

    print(f"Writing appointments Delta table → {appt_path}")
    write_deltalake(
        appt_path,
        appt_df,
        storage_options=STORAGE_OPTIONS,
        mode="overwrite",
    )

    print(f"Writing patients Delta table → {patient_path}")
    write_deltalake(
        patient_path,
        patient_df,
        storage_options=STORAGE_OPTIONS,
        mode="overwrite",
    )

    # Verify
    appt_count = len(DeltaTable(appt_path, storage_options=STORAGE_OPTIONS).to_pandas())
    pat_count = len(DeltaTable(patient_path, storage_options=STORAGE_OPTIONS).to_pandas())
    print(f"Seeded: {appt_count} appointments, {pat_count} patients. Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rows", type=int, default=2000, help="Number of appointment rows to generate"
    )
    args = parser.parse_args()
    main(args.rows)
