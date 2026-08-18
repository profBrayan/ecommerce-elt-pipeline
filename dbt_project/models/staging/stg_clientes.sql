-- Dedup por cliente_id (mantém o registro mais recente) + tipagem/normalização.
-- Nulos em cidade/estado/segmento são preservados como nulos reais (não são
-- "críticos" para o mart, mas ficam sinalizados via segmento='nao_informado'
-- quando o dado de segmento está ausente, já que o mart agrupa por segmento).
with source as (
    select * from {{ source('raw', 'clientes') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by cliente_id
            order by _ingestion_date desc, data_cadastro desc
        ) as rn
    from source
    where cliente_id is not null
)

select
    cliente_id,
    nullif(trim(nome), '') as nome,
    nullif(trim(cpf), '') as cpf,
    lower(nullif(trim(email), '')) as email,
    nullif(trim(cidade), '') as cidade,
    upper(nullif(trim(estado), '')) as estado,
    safe_cast(data_cadastro as date) as data_cadastro,
    coalesce(nullif(trim(segmento), ''), 'nao_informado') as segmento
from deduplicated
where rn = 1
