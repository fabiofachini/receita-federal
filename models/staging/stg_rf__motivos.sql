-- models/staging/

with motivos as (
    select * 
    from {{ source('main', 'src_motivos') }}
),

-- Transformação dos dados
stg_rf__motivos as (
    select
        cast("0" as int) as motivos_codigo,
        cast("1" as varchar(200)) as motivos_descricao
    from motivos
)

-- Retorno dos dados transformados
select * from stg_rf__motivos