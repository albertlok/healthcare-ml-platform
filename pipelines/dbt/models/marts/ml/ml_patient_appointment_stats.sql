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

-- Anchor: one row per patient per completed/no-show appointment (the prediction target)
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

-- Rolling 30-day statistics using window functions
-- Point-in-time correct: only looks at appointments BEFORE the current one
stats_30d as (
    select
        patient_id,
        appointment_id,
        scheduled_at,
        -- Exclude the current appointment from the lookback window
        avg(is_no_show::int) over (
            partition by patient_id
            order by scheduled_at
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

-- Days since last completed appointment (recency feature)
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
        pa.scheduled_at                                     as feature_timestamp,
        pa.is_no_show                                       as label,

        -- 30-day features (coalesce to 0 for first-time patients)
        coalesce(s30.no_show_rate_30d, 0.0)::float          as no_show_rate_30d,
        coalesce(s30.cancellation_rate_30d, 0.0)::float      as cancellation_rate_30d,

        -- 90-day features
        coalesce(s90.no_show_rate_90d, 0.0)::float          as no_show_rate_90d,
        coalesce(s90.cancellation_rate_90d, 0.0)::float      as cancellation_rate_90d,
        coalesce(s90.total_appointments_90d, 0)::int         as total_appointments_90d,
        coalesce(s90.avg_lead_time_days, 0.0)::float         as avg_lead_time_days,

        -- Recency
        coalesce(r.last_appointment_days_ago, 999)::int      as last_appointment_days_ago,

        current_timestamp                                   as _dbt_updated_at

    from patient_appointments pa
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
