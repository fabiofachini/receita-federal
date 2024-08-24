-- models/staging/

with municipios as (
    select * 
    from {{ source('main', 'src_municipios') }}
),

-- Transformação dos dados
stg_rf__municipios as (
    select
        cast("0" as int) as municipios_codigo,
        cast("1" as varchar(200)) as municipios_descricao
    from municipios
)

-- Retorno dos dados transformados
select * from stg_rf__municipios