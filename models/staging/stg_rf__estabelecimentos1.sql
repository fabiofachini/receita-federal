-- models/staging/

with estabelecimentos as (
    select * 
    from {{ source('main', 'src_estabelecimentos1') }}
),

-- Transformação dos dados
stg_rf__estabelecimentos1 as (
    select
        cast("0" as varchar(8)) as cnpj_basico,
        cast("1" as varchar(4)) as cnpj_ordem,
        cast("2" as varchar(2)) as cnpj_dv,
        cast("3" as int) as matriz_filial,
        cast("4" as varchar(200)) as nome_fantasia,
        cast("5" as int) as situacao_cadastral,
        -- Transformação da data com verificação de formato
        case 
            when length("6") = 8 and regexp_replace("6", '\D', '') = "6" then 
                cast(substr("6", 1, 4) || '-' || substr("6", 5, 2) || '-' || substr("6", 7, 2) as date)
            else 
                NULL
        end as situacao_data,
        cast("7" as int) as situacao_motivo,
        cast("8" as varchar(100)) as cidade_exterior,
        cast("9" as int) as pais,
        case 
            when length("10") = 8 and regexp_replace("10", '\D', '') = "10" then 
                cast(substr("10", 1, 4) || '-' || substr("10", 5, 2) || '-' || substr("10", 7, 2) as date)
            else 
                NULL
        end as data_inicio,
        cast("11" as int) as cnae_principal,
        cast("12" as varchar(100)) as cnae_secundario,
        cast("13" as varchar(20)) as logradouro_tipo,
        cast("14" as varchar(200)) as logradouro,
        cast("15" as varchar(15)) as logradouro_numero,
        cast("16" as varchar(30)) as logradouro_complemento,
        cast("17" as varchar(200)) as bairro,
        cast("18" as varchar(8)) as cep,
        cast("19" as varchar(2)) as uf,
        cast("20" as int) as municipio,
        cast("21" as varchar(4)) as ddd1,
        cast("22" as varchar(10)) as telefone1,
        cast("23" as varchar(4)) as ddd2,
        cast("24" as varchar(10)) as telefone2,
        cast("25" as varchar(4)) as dddfax,
        cast("26" as varchar(10)) as fax,
        cast("27" as varchar(200)) as email,
        cast("28" as varchar(50)) as situacao_especial,
        case 
            when length("29") = 8 and regexp_replace("29", '\D', '') = "29" then 
                cast(substr("29", 1, 4) || '-' || substr("29", 5, 2) || '-' || substr("29", 7, 2) as date)
            else 
                NULL
        end as situacao_especial_data
    from estabelecimentos
)

-- Retorno dos dados transformados
select * from stg_rf__estabelecimentos1