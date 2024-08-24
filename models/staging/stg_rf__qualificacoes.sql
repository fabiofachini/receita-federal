-- models/staging/

with qualificacoes as (
    select * 
    from {{ source('main', 'src_qualificacoes') }}
),

-- Transformação dos dados
stg_rf__qualificacoes as (
    select
        cast("0" as int) as qualificacoes_codigo,
        cast("1" as varchar(200)) as qualificacoes_descricao
    from qualificacoes
)

-- Retorno dos dados transformados
select * from stg_rf__qualificacoes
