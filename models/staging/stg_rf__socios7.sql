-- models/staging/stg_bacen__carteira_de_credito_pf.sql

with carteira_de_credito_pf as (
    select * from {{ source('dbo', 'carteira_de_credito_pf') }}
),

-- transformação dos dados
stg_bacen__carteira_de_credito_pf as (
    select
        CONVERT(DATE, data, 103) AS Data,
        cast(valor as int) as Carteira_de_Credito_PF
    from carteira_de_credito_pf
)

-- retorno dos dados transformados
select * from stg_bacen__carteira_de_credito_pf
