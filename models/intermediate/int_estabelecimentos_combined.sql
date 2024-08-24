with int_estabelecimentos_combined as (
    select * from {{ ref('stg_rf__estabelecimentos0') }}
    union all
    select * from {{ ref('stg_rf__estabelecimentos1') }}
    union all
    select * from {{ ref('stg_rf__estabelecimentos2') }}
    union all
    select * from {{ ref('stg_rf__estabelecimentos3') }}
    union all
    select * from {{ ref('stg_rf__estabelecimentos4') }}
    union all
    select * from {{ ref('stg_rf__estabelecimentos5') }}
    union all
    select * from {{ ref('stg_rf__estabelecimentos6') }}
    union all
    select * from {{ ref('stg_rf__estabelecimentos7') }}
    union all
    select * from {{ ref('stg_rf__estabelecimentos8') }}
    union all
    select * from {{ ref('stg_rf__estabelecimentos9') }}
)

select * from int_estabelecimentos_combined
