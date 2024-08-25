-- Contagem de estabelecimentos ativos no Brasil

with _estabelecimentos_count as (
    select * from {{ ref('int_estabelecimentos_combined') }}


select 
    count(*) 
    from _estabelecimentos_count 
    where situacao_cadastral = 2
