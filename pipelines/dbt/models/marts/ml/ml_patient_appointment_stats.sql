-- ML feature mart: patient appointment behavioral statistics.
-- This model is the primary data source for Feast. It is materialized as a TABLE
-- (not a view) so Feast can export it to Parquet without executing a long query each time.
-- The 'feast-source' tag marks it as a Feast input for documentation and CI filtering.
--
-- materialized='table' persists results in DuckDB/Snowflake instead of re-running
-- the query on every SELECT. For this model with window functions over large history,
-- a view would be very slow. Tables trade storage for query speed.

{{
    config(
        materialized='table',
        tags=['ml', 'feast-source'],
        post_hook="{{ log('Refreshed ml_patient_appointment_stats', info=true) }}"
    )
}}

with appointments as (
    select * from {{ ref('stg_appointments') }}
),

-- Anchor CTE: one row per final appointment outcome (COMPLETED, NO_SHOW, or CANCELLED).
-- Each row becomes a training example. SCHEDULED and RESCHEDULED are excluded because
-- they don't have an outcome yet — we can't label them.
patient_appointments as (
    select
        patient_id,
        appointment_id,
        scheduled_at,
        is_no_show,
        is_cancelled,
        is_completed,
        lead_time_hours
    from appointments
    where event_type in ('COMPLETED', 'NO_SHOW', 'CANCELLED')
),

-- Rolling 30-day window functions.
-- The key insight: "range between ... '1 second' preceding" excludes the current row
-- from its own lookback window. This is the point-in-time correctness guarantee:
-- when computing no_show_rate for appointment X, we only use appointments that
-- happened BEFORE X — not X itself. Including the current row would be data leakage.
stats_30d as (
    select
        patient_id,
        appointment_id,
        scheduled_at,
        avg(is_no_show::int) over (
            partition by patient_id
            order by scheduled_at
            -- "range between N preceding and 1 second preceding" means:
            -- sum over all rows for this patient where scheduled_at is between
            -- (current_scheduled_at - 30 days) and (current_scheduled_at - 1 second).
            -- The "1 second preceding" upper bound excludes same-timestamp rows.
            range between interval '30 days' preceding and interval '1 second' preceding
        )                                                   as no_show_rate_30d,
        avg(is_cancelled::int) over (
            partition by patient_id
            order by scheduled_at
            range between interval '30 days' preceding and interval '1 second' preceding
        )                                                   as cancellation_rate_30d
    from patient_appointments
),

-- Rolling 90-day statistics
stats_90d as (
    select
        patient_id,
        appointment_id,
        scheduled_at,
        avg(is_no_show::int) over (
            partition by patient_id
            order by scheduled_at
            range between interval '90 days' preceding and interval '1 second' preceding
        )                                                   as no_show_rate_90d,
        avg(is_cancelled::int) over (
            partition by patient_id
            order by scheduled_at
            range between interval '90 days' preceding and interval '1 second' preceding
        )                                                   as cancellation_rate_90d,
        count(*) over (
            partition by patient_id
            order by scheduled_at
            range between interval '90 days' preceding and interval '1 second' preceding
        )                                                   as total_appointments_90d,
        avg(lead_time_hours::float / 24.0) over (
            partition by patient_id
            order by scheduled_at
            range between interval '90 days' preceding and interval '1 second' preceding
        )                                                   as avg_lead_time_days
    from patient_appointments
),

-- Recency feature: how many days since this patient's last appointment?
-- High values suggest a patient who hasn't been seen in a long time —
-- potentially disengaged, which correlates with higher no-show rates.
-- We use lag() instead of a window range so we get the immediately preceding
-- appointment's date rather than a count over an interval.
recency as (
    select
        patient_id,
        appointment_id,
        scheduled_at,
        datediff(
            'day',
            lag(scheduled_at) over (partition by patient_id order by scheduled_at),
            scheduled_at
        )                                                   as last_appointment_days_ago
    from patient_appointments
    where is_completed = true or is_no_show = true
),

final as (
    select
        pa.patient_id,
        pa.appointment_id,
        -- feature_timestamp is what Feast uses for point-in-time joins.
        -- It represents "at this moment in time, what were the feature values?"
        pa.scheduled_at                                     as feature_timestamp,
        -- label = the training target: 1 if the patient no-showed, 0 otherwise.
        pa.is_no_show                                       as label,

        -- coalesce to 0 for first-time patients who have no prior history.
        -- Null features would cause XGBoost to use the average value (fine) or crash
        -- depending on the version. Explicit 0 is safer and more interpretable.
        coalesce(s30.no_show_rate_30d, 0.0)::float          as no_show_rate_30d,
        coalesce(s30.cancellation_rate_30d, 0.0)::float      as cancellation_rate_30d,

        coalesce(s90.no_show_rate_90d, 0.0)::float          as no_show_rate_90d,
        coalesce(s90.cancellation_rate_90d, 0.0)::float      as cancellation_rate_90d,
        coalesce(s90.total_appointments_90d, 0)::int         as total_appointments_90d,
        coalesce(s90.avg_lead_time_days, 0.0)::float         as avg_lead_time_days,

        -- 999 days is our sentinel for "never had a prior appointment."
        -- It's intentionally a large value so the model treats this as a distinct
        -- signal (new patient) rather than a small recency value.
        coalesce(r.last_appointment_days_ago, 999)::int      as last_appointment_days_ago,

        current_timestamp                                   as _dbt_updated_at

    from patient_appointments pa
    -- Left joins so patients with no prior history still appear (with coalesced 0s above)
    left join stats_30d s30
        on pa.patient_id = s30.patient_id
        and pa.appointment_id = s30.appointment_id
    left join stats_90d s90
        on pa.patient_id = s90.patient_id
        and pa.appointment_id = s90.appointment_id
    left join recency r
        on pa.patient_id = r.patient_id
        and pa.appointment_id = r.appointment_id
)

select * from final
