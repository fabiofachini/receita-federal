-- models/staging/

with cnaes as (
    select * 
    from {{ source('main', 'src_cnaes') }}
),

-- Transformação dos dados
stg_rf__cnaes as (
    select
        cast("0" as integer) as cnaes_codigo,
        cast("1" as varchar(200)) as cnaes_descricao
    from cnaes
)

-- Retorno dos dados transformados
select * from stg_rf__cnaes

