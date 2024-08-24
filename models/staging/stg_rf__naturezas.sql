-- models/staging/

with naturezas as (
    select * 
    from {{ source('main', 'src_naturezas') }}
),

-- Transformação dos dados
stg_rf__naturezas as (
    select
        cast("0" as int) as naturezas_codigo,
        cast("1" as varchar(200)) as naturezas_descricao
    from naturezas
)

-- Retorno dos dados transformados
select * from stg_rf__naturezas