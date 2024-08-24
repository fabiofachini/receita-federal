-- models/staging/

with socios as (
    select * 
    from {{ source('main', 'src_socios6') }}
),

-- Transformação dos dados
stg_rf__socios6 as (
    select
        cast("0" as varchar(8)) as cnpj_basico,
        cast("1" as integer) as identificador_socio,
        cast("2" as varchar(100)) as nome_socio,
        cast("3" as varchar(14)) as cpf_cnpj_socio,
        cast("4" as integer) as qualificacao_socio,
        case 
            when length("5") = 8 and regexp_replace("5", '\D', '') = "5" then 
                cast(substr("5", 1, 4) || '-' || substr("5", 5, 2) || '-' || substr("5", 7, 2) as date)
            else 
                NULL
        end as data_entrada_sociedade,
        cast("6" as integer) as pais,
        cast("7" as varchar(14)) as representante_legal_cpf,
        cast("8" as varchar(100)) as representante_legal_nome,
        cast("9" as integer) as representante_legal_qualificacao,
        cast("10" as integer) as faixa_etaria
    from socios
)

-- Retorno dos dados transformados
select * from stg_rf__socios6