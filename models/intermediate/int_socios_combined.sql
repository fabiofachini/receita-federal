with int_socios_combined as (
    select * from {{ ref('stg_rf__socios0') }}
    union all
    select * from {{ ref('stg_rf__socios1') }}
    union all
    select * from {{ ref('stg_rf__socios2') }}
    union all
    select * from {{ ref('stg_rf__socios3') }}
    union all
    select * from {{ ref('stg_rf__socios4') }}
    union all
    select * from {{ ref('stg_rf__socios5') }}
    union all
    select * from {{ ref('stg_rf__socios6') }}
    union all
    select * from {{ ref('stg_rf__socios7') }}
    union all
    select * from {{ ref('stg_rf__socios8') }}
    union all
    select * from {{ ref('stg_rf__socios9') }}
)

select * from int_socios_combined
