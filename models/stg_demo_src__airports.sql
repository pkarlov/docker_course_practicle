
with source as (

    select * from {{ source('demo_src', 'airports') }}

),

renamed as (

    select
        airport_code,
        airport_name,
        city,
        coordinates,
        timezone

    from source

)

select * from renamed

