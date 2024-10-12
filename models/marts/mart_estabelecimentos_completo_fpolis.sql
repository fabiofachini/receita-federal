-- Selecao dos estabelecimentos ativos do Brasil

select 	
    estabelecimentos.cnpj_basico || estabelecimentos.cnpj_ordem || estabelecimentos.cnpj_dv as cnpj,
    empresas.razao_social as razao_social,
    estabelecimentos.nome_fantasia as nome_fantasia,
    case
        when estabelecimentos.matriz_filial = '1' then 'matriz'
        when estabelecimentos.matriz_filial = '2' then 'filial'
        else 'nao informado'
    end as matriz_filial,
    case
        when simples.opcao_mei = 'S' then 'mei'
        when empresas.porte = '00' then 'nao informado'
        when empresas.porte = '01' then 'micro empresa'
        when empresas.porte = '03' then 'empresa de pequeno porte'
        when empresas.porte = '05' then 'demais portes'
        else 'nao informado'
    end as porte,
    empresas.capital_social as capital_social,
    estabelecimentos.uf as estado,
    municipio.municipios_descricao as municipio,
    estabelecimentos.bairro as bairro,
    estabelecimentos.cep as cep,
    estabelecimentos.logradouro_tipo as logradouro_tipo,
    estabelecimentos.logradouro as logradouro,
    estabelecimentos.logradouro_numero as numero,
    estabelecimentos.logradouro_tipo || ' ' || estabelecimentos.logradouro || ', ' || estabelecimentos.logradouro_numero 
    || ', ' ||  estabelecimentos.bairro || ', ' || municipio.municipios_descricao
    || ', ' || estabelecimentos.uf || ', ' || 'BRASIL'|| ', ' ||"CEP"|| ' ' ||estabelecimentos.cep as endereco,
    estabelecimentos.cnae_principal as cod_cnae,
    upper(cnae.cnaes_descricao) as cnae_principal,
    case
        when cast(estabelecimentos.cnae_principal as int) <= 322199 then 'agricultura'
        when cast(estabelecimentos.cnae_principal as int) between 500301 and 3900500 then 'industria'
        when cast(estabelecimentos.cnae_principal as int) between 4110700 and 4399199 then 'construcao'
        when cast(estabelecimentos.cnae_principal as int) between 4511101 and 4789099 then 'comercio'
        when cast(estabelecimentos.cnae_principal as int) between 4911600 and 8888887 then 'servicos'
        when cast(estabelecimentos.cnae_principal as int) >= 8888889 then 'servicos'
        else 'nao_informado'
    end as setor,
    case
        when cast(estabelecimentos.cnae_principal as int) <= 322199 then 'agricultura, pecuaria, producao florestal, pesca e aquicultura'
        when cast(estabelecimentos.cnae_principal as int) between 500301 and 990403 then 'industrias extrativas'
        when cast(estabelecimentos.cnae_principal as int) between 1011201 and 3329599 then 'industrias de transformacao'
        when cast(estabelecimentos.cnae_principal as int) between 3511501 and 3530100 then 'eletricidade e gas'
        when cast(estabelecimentos.cnae_principal as int) between 3600601 and 3900500 then 'agua, esgoto, atividades de gestao de residuos e descontaminacao'
        when cast(estabelecimentos.cnae_principal as int) between 4110700 and 4399199 then 'construcao'
        when cast(estabelecimentos.cnae_principal as int) between 4511101 and 4789099 then 'comercio; reparacao de veiculos automotores e motocicletas'
        when cast(estabelecimentos.cnae_principal as int) between 4911600 and 5320202 then 'transporte, armazenagem e correio'
        when cast(estabelecimentos.cnae_principal as int) between 5510801 and 5620104 then 'alojamento e alimentacao'
        when cast(estabelecimentos.cnae_principal as int) between 5811500 and 6399200 then 'informacao e comunicacao'
        when cast(estabelecimentos.cnae_principal as int) between 6410700 and 6630400 then 'atividades financeiras, de seguros e servicos relacionados'
        when cast(estabelecimentos.cnae_principal as int) between 6810201 and 6822600 then 'atividades imobiliarias'
        when cast(estabelecimentos.cnae_principal as int) between 6911701 and 7500100 then 'atividades profissionais, cientificas e tecnicas'
        when cast(estabelecimentos.cnae_principal as int) between 7711000 and 8299799 then 'atividades administrativas e servicos complementares'
        when cast(estabelecimentos.cnae_principal as int) between 8411600 and 8430200 then 'administracao publica, defesa e seguridade social'
        when cast(estabelecimentos.cnae_principal as int) between 8511200 and 8599699 then 'educacao'
        when cast(estabelecimentos.cnae_principal as int) between 8610101 and 8800600 then 'saude humana e servicos sociais'
        when cast(estabelecimentos.cnae_principal as int) between 9001901 and 9329899 then 'artes, cultura, esporte e recreacao'
        when cast(estabelecimentos.cnae_principal as int) between 9411100 and 9609299 then 'outras atividades de servicos'
        when cast(estabelecimentos.cnae_principal as int) = 9700500 then 'servicos domesticos'
        when cast(estabelecimentos.cnae_principal as int) = 9900800 then 'organismos internacionais e outras instituicoes extraterritoriais'
        when cast(estabelecimentos.cnae_principal as int) = 8888888 then 'nao informado'
        else 'nao informado'
    end as classe,
    estabelecimentos.ddd1 || estabelecimentos.telefone1 as telefone_1,
    estabelecimentos.ddd2 || estabelecimentos.telefone2 as telefone_2,
    estabelecimentos.email as email

from {{ ref('int_estabelecimentos_combined') }} as estabelecimentos

left join {{ ref('int_empresas_combined') }} as empresas
on estabelecimentos.cnpj_basico = empresas.cnpj_basico

left join {{ ref('stg_rf__simples') }} as simples
on estabelecimentos.cnpj_basico = simples.cnpj_basico

left join {{ ref('stg_rf__cnaes') }} as cnae
on estabelecimentos.cnae_principal = cnae.cnaes_codigo

left join {{ ref('stg_rf__municipios') }} as municipio
on estabelecimentos.municipio = municipio.municipios_codigo

where estabelecimentos.situacao_cadastral = '02' and municipio.municipios_codigo = '8105'
