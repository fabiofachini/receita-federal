-- Contagem de estabelecimentos ativos no Brasil

with estabelecimentos_cidade_count as (
    select * 
    from {{ ref('int_estabelecimentos_combined') }}
)

-- Contagem de estabelecimentos ativos por cidade

select 
    stg_rf__municipios.municipios_descricao,
    count(*) as empresas_ativas
from estabelecimentos_cidade_count
left join stg_rf__municipios 
    on estabelecimentos_cidade_count.municipio = stg_rf__municipios.municipios_codigo
where estabelecimentos_cidade_count.situacao_cadastral = '02'
group by stg_rf__municipios.municipios_descricao
order by count(*) desc

