-- models/staging/

with empresas as (
    select * 
    from {{ source('main', 'src_empresas3') }}
),

-- Transformação dos dados
stg_rf__empresas3 as (
    select
        cast("0" as varchar(8)) as cnpj_basico,
        cast("1" as varchar(200)) as razao_social,
        cast("2" as int) as natureza_juridica,
        cast("3" as int) as qualificacao_responsavel,
        cast(replace("4", ',', '.') as decimal(20,2)) as capital_social,
        cast("5" as int) as porte,
        cast("6" as varchar(50)) as ente_federativo
    from empresas
)

-- Retorno dos dados transformados
select * from stg_rf__empresas3
