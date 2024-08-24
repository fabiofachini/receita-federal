-- models/staging/

with simples as (
    select * 
    from {{ source('main', 'src_simples') }}
),

-- Transformação dos dados
stg_rf__simples as (
    select
        cast("0" as varchar(8)) as cnpj_basico,
        cast("1" as char(1)) as opcao_simples,
        case 
            when length("2") = 8 and regexp_replace("2", '\D', '') = "2" and substr("2", 1, 4) >= '1900' and substr("2", 5, 2) between '01' and '12' and substr("2", 7, 2) between '01' and '31' then 
                cast(substr("2", 1, 4) || '-' || substr("2", 5, 2) || '-' || substr("2", 7, 2) as date)
            else 
                NULL
        end as opcao_simples_data,
        case 
            when length("3") = 8 and regexp_replace("3", '\D', '') = "3" and substr("3", 1, 4) >= '1900' and substr("3", 5, 2) between '01' and '12' and substr("3", 7, 2) between '01' and '31' then 
                cast(substr("3", 1, 4) || '-' || substr("3", 5, 2) || '-' || substr("3", 7, 2) as date)
            else 
                NULL
        end as exclusao_simples_data,
        cast("4" as char(1)) as opcao_mei,
        case 
            when length("5") = 8 and regexp_replace("5", '\D', '') = "5" and substr("5", 1, 4) >= '1900' and substr("5", 5, 2) between '01' and '12' and substr("5", 7, 2) between '01' and '31' then 
                cast(substr("5", 1, 4) || '-' || substr("5", 5, 2) || '-' || substr("5", 7, 2) as date)
            else 
                NULL
        end as opcao_mei_data,
        case 
            when length("6") = 8 and regexp_replace("6", '\D', '') = "6" and substr("6", 1, 4) >= '1900' and substr("6", 5, 2) between '01' and '12' and substr("6", 7, 2) between '01' and '31' then 
                cast(substr("6", 1, 4) || '-' || substr("6", 5, 2) || '-' || substr("6", 7, 2) as date)
            else 
                NULL
        end as exclusao_mei_data
    from simples
)

-- Retorno dos dados transformados
select * from stg_rf__simples
