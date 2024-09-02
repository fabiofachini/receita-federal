-- Contagem de setores dos estabelecimentos ativos do Brasil

with estabelecimentos_ativos as (
    select
        case
            when cast(estabelecimentos.cnae_principal as int) <= 322199 then 'agricultura'
            when cast(estabelecimentos.cnae_principal as int) between 500301 and 3900500 then 'industria'
            when cast(estabelecimentos.cnae_principal as int) between 4110700 and 4399199 then 'construcao'
            when cast(estabelecimentos.cnae_principal as int) between 4511101 and 4789099 then 'comercio'
            when cast(estabelecimentos.cnae_principal as int) between 4911600 and 8888887 then 'servicos'
            when cast(estabelecimentos.cnae_principal as int) >= 8888889 then 'servicos'
            else 'nao_informado'
        end as setor,
        count(*) as total_estabelecimentos
    from {{ ref('int_estabelecimentos_combined') }} as estabelecimentos
    where estabelecimentos.situacao_cadastral = '02'
    group by setor
)

select
    setor,
    total_estabelecimentos
from estabelecimentos_ativos
order by total_estabelecimentos desc
