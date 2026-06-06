with

source as (
    -- Raw appointments from Delta Lake silver layer
    select * from {{ source('silver', 'appointments') }}
    where _dq_passed = true
),

renamed as (
    select
        -- Keys
        appointment_id::varchar                             as appointment_id,
        patient_id::varchar                                 as patient_id,
        provider_id::varchar                                as provider_id,
        clinic_id::varchar                                  as clinic_id,

        -- Enums (already uppercased in silver)
        event_type::varchar                                 as event_type,
        appointment_type::varchar                           as appointment_type,
        insurance_type::varchar                             as insurance_type,

        -- Timestamps
        event_timestamp_ts::timestamp                       as event_at,
        scheduled_start_ts::timestamp                       as scheduled_at,

        -- Numerics
        scheduled_duration_minutes::int                     as duration_minutes,
        copay_amount_usd::decimal(10, 2)                    as copay_usd,
        lead_time_hours::int                                as lead_time_hours,

        -- Booleans
        is_reminder_sent::boolean                           as is_reminder_sent,
        is_no_show::boolean                                 as is_no_show,
        is_cancelled::boolean                               as is_cancelled,
        is_completed::boolean                               as is_completed,
        is_morning_appointment::boolean                     as is_morning_appointment,

        -- Derived temporal
        scheduled_day_of_week::int                          as day_of_week,
        scheduled_hour::int                                 as hour_of_day,
        scheduled_month::int                                as appointment_month,
        lead_time_category::varchar                         as lead_time_category,

        -- Audit
        _silver_processed_at::timestamp                     as _silver_processed_at,
        _partition_date::date                               as partition_date

    from source
)

select * from renamed
