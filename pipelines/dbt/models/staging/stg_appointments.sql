-- Staging models are the "contract" between raw source data and dbt transformations.
-- They do one thing: rename columns to a consistent standard and cast to authoritative types.
-- No business logic lives here — that belongs in intermediate or mart models.
-- The advantage: if the source column changes (e.g., silver renames a field), you fix
-- it in one place (the staging model) and every downstream model is automatically correct.

with

source as (
    -- Raw appointments from Delta Lake silver layer.
    -- _dq_passed=true filters out rows that failed the Spark data quality check,
    -- so everything downstream can assume clean data.
    select * from {{ source('silver', 'appointments') }}
    where _dq_passed = true
),

renamed as (
    -- All explicit casts serve two purposes:
    -- 1. Document the canonical type for each column (the source of truth)
    -- 2. Catch schema drift early — if silver changes a type, the cast fails loudly here
    --    rather than silently corrupting a gold model downstream.
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

        -- Timestamps renamed to drop the '_ts' suffix — cleaner downstream column names
        event_timestamp_ts::timestamp                       as event_at,
        scheduled_start_ts::timestamp                       as scheduled_at,

        -- Numerics
        scheduled_duration_minutes::int                     as duration_minutes,
        copay_amount_usd::decimal(10, 2)                    as copay_usd,
        lead_time_hours::int                                as lead_time_hours,

        -- Booleans: is_/has_ prefix makes boolean semantics obvious in downstream SQL
        is_reminder_sent::boolean                           as is_reminder_sent,
        is_no_show::boolean                                 as is_no_show,
        is_cancelled::boolean                               as is_cancelled,
        is_completed::boolean                               as is_completed,
        is_morning_appointment::boolean                     as is_morning_appointment,

        -- Derived temporal (computed in Spark silver, surfaced here as typed ints)
        scheduled_day_of_week::int                          as day_of_week,
        scheduled_hour::int                                 as hour_of_day,
        scheduled_month::int                                as appointment_month,
        lead_time_category::varchar                         as lead_time_category,

        -- Audit columns: keep the lineage trail from silver into gold
        _silver_processed_at::timestamp                     as _silver_processed_at,
        _partition_date::date                               as partition_date

    from source
)

select * from renamed
