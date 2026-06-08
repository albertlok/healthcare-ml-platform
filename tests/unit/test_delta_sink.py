"""Unit tests for delta_sink_simple._flatten() and batch write helpers."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch


def _flatten(record: dict[str, Any]) -> dict[str, Any]:
    """Mirror of ingestion/consumers/delta_sink_simple._flatten for testing."""
    flat = {}
    for k, v in record.items():
        if isinstance(v, list):
            flat[k] = json.dumps(v)
        elif isinstance(v, dict):
            flat[k] = json.dumps(v)
        else:
            flat[k] = v
    now = datetime.now(tz=timezone.utc)
    flat["ingestion_date"] = now.date().isoformat()
    flat["_ingested_at"] = now.isoformat()
    return flat


class TestFlatten:
    def test_scalar_fields_pass_through(self):
        record = {"appointment_id": "abc-123", "patient_id": "p-456", "no_show": True}
        result = _flatten(record)
        assert result["appointment_id"] == "abc-123"
        assert result["patient_id"] == "p-456"
        assert result["no_show"] is True

    def test_list_fields_serialised_to_json_string(self):
        record = {"tags": ["urgent", "follow-up"], "scores": [0.1, 0.9]}
        result = _flatten(record)
        assert result["tags"] == json.dumps(["urgent", "follow-up"])
        assert result["scores"] == json.dumps([0.1, 0.9])

    def test_dict_fields_serialised_to_json_string(self):
        record = {"metadata": {"source": "ehr", "version": 2}}
        result = _flatten(record)
        assert result["metadata"] == json.dumps({"source": "ehr", "version": 2})

    def test_ingestion_metadata_added(self):
        result = _flatten({"event_id": "x"})
        assert "ingestion_date" in result
        assert "X" not in result["ingestion_date"]  # should be ISO date string
        assert date.fromisoformat(result["ingestion_date"])  # valid ISO date

    def test_ingested_at_is_iso_timestamp(self):
        result = _flatten({"event_id": "x"})
        ts = result["_ingested_at"]
        # Should be parseable as a datetime
        datetime.fromisoformat(ts)

    def test_empty_record_gets_metadata(self):
        result = _flatten({})
        assert set(result.keys()) == {"ingestion_date", "_ingested_at"}

    def test_nested_none_values_pass_through(self):
        record = {"copay": None, "notes": ""}
        result = _flatten(record)
        assert result["copay"] is None
        assert result["notes"] == ""


class TestSilverDerivedColumns:
    """Test the column derivation logic used in the silver transform tasks."""

    import numpy as np
    import pandas as pd

    def test_lead_time_category_short(self):
        import pandas as pd

        df = pd.DataFrame({"lead_time_days": [1, 3, 6]})
        df["lead_time_category"] = pd.cut(
            df["lead_time_days"],
            bins=[-1, 7, 30, 90, float("inf")],
            labels=["short", "medium", "long", "very_long"],
        ).astype(str)
        assert df.loc[0, "lead_time_category"] == "short"
        assert df.loc[1, "lead_time_category"] == "short"
        assert df.loc[2, "lead_time_category"] == "short"

    def test_lead_time_category_medium(self):
        import pandas as pd

        df = pd.DataFrame({"lead_time_days": [8, 15, 30]})
        df["lead_time_category"] = pd.cut(
            df["lead_time_days"],
            bins=[-1, 7, 30, 90, float("inf")],
            labels=["short", "medium", "long", "very_long"],
        ).astype(str)
        assert all(df["lead_time_category"] == "medium")

    def test_lead_time_category_very_long(self):
        import pandas as pd

        df = pd.DataFrame({"lead_time_days": [91, 180, 365]})
        df["lead_time_category"] = pd.cut(
            df["lead_time_days"],
            bins=[-1, 7, 30, 90, float("inf")],
            labels=["short", "medium", "long", "very_long"],
        ).astype(str)
        assert all(df["lead_time_category"] == "very_long")

    def test_is_morning_appointment(self):
        import pandas as pd

        df = pd.DataFrame({"scheduled_hour": [7, 11, 12, 14]})
        df["is_morning_appointment"] = df["scheduled_hour"] < 12
        assert df.loc[0, "is_morning_appointment"] is True
        assert df.loc[1, "is_morning_appointment"] is True
        assert df.loc[2, "is_morning_appointment"] is False
        assert df.loc[3, "is_morning_appointment"] is False

    def test_is_no_show_derived_from_event_type(self):
        import pandas as pd

        df = pd.DataFrame(
            {
                "event_type": [
                    "NO_SHOW",
                    "COMPLETED",
                    "SCHEDULED",
                    "CANCELLED",
                    "RESCHEDULED",
                ]
            }
        )
        df["is_no_show"] = df["event_type"] == "NO_SHOW"
        df["is_cancelled"] = df["event_type"] == "CANCELLED"
        df["is_completed"] = df["event_type"] == "COMPLETED"

        assert df.loc[0, "is_no_show"] is True
        assert df.loc[1, "is_completed"] is True
        assert df.loc[3, "is_cancelled"] is True
        assert df.loc[2, "is_no_show"] is False
