-- models/staging/

with paises as (
    select * 
    from {{ source('main', 'src_paises') }}
),

-- Transformação dos dados
stg_rf__paises as (
    select
        cast("0" as int) as paises_codigo,
        cast("1" as varchar(200)) as paises_descricao
    from paises
)

-- Retorno dos dados transformados
select * from stg_rf__paises