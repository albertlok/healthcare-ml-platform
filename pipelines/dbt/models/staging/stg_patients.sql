-- Staging model for the SCD2 patients dimension.
-- We filter to is_current=true here so all downstream models always see the latest
-- patient state. If you ever need historical patient attributes (e.g., "what was this
-- patient's insurance type when they had their appointment in 2023?"), you can create
-- a separate stg_patients_history model that joins on valid_from/valid_to instead.

with

source as (
    -- Only current patient rows from the SCD2 table.
    -- SCD2 stores one row per change event — multiple rows per patient exist, but
    -- only one has is_current=true (the most recent state). This filter collapses
    -- history into a single "current view" of each patient.
    select * from {{ source('silver', 'patients') }}
    where is_current = true
),

renamed as (
    select
        patient_id::varchar                                 as patient_id,
        gender::varchar                                     as gender,
        zip_code::varchar                                   as zip_code,
        state::varchar                                      as state,
        insurance_type::varchar                             as insurance_type,
        insurance_plan_name::varchar                        as insurance_plan_name,
        preferred_language::varchar                         as preferred_language,
        preferred_communication_channel::varchar            as preferred_communication_channel,
        distance_to_clinic_miles::decimal(6, 2)            as distance_to_clinic_miles,
        has_chronic_condition::boolean                      as has_chronic_condition,
        registration_source::varchar                        as registration_source,

        -- Convert epoch days → date
        date_of_birth_date::date                            as date_of_birth,

        -- SCD2 tracking
        valid_from::timestamp                               as valid_from,
        valid_to::timestamp                                 as valid_to,
        is_current::boolean                                 as is_current,

        _silver_processed_at::timestamp                     as _silver_processed_at

    from source
)

select * from renamed
