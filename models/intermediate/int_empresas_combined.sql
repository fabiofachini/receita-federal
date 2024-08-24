with empresas_combined as (
    select * from {{ ref('stg_rf__empresas0') }}
    union all
    select * from {{ ref('stg_rf__empresas1') }}
    union all
    select * from {{ ref('stg_rf__empresas2') }}
    union all
    select * from {{ ref('stg_rf__empresas3') }}
    union all
    select * from {{ ref('stg_rf__empresas4') }}
    union all
    select * from {{ ref('stg_rf__empresas5') }}
    union all
    select * from {{ ref('stg_rf__empresas6') }}
    union all
    select * from {{ ref('stg_rf__empresas7') }}
    union all
    select * from {{ ref('stg_rf__empresas8') }}
    union all
    select * from {{ ref('stg_rf__empresas9') }}
),

ranked_empresas as (
    select *,
           row_number() over (partition by cnpj_basico order by (select null)) as rn
    from empresas_combined
),

int_empresas_combined as (
    select
        cnpj_basico,
        razao_social,
        natureza_juridica,
        qualificacao_responsavel,
        capital_social,
        porte,
        ente_federativo
    from ranked_empresas
    where rn = 1
)

select * from int_empresas_combined

