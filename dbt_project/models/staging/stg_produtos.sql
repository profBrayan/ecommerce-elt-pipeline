-- Dedup por produto_id + normalização de dois campos propositalmente sujos:
--   preco_tabela: texto misto "R$ 533.46" / "765.78" -> NUMERIC
--   ativo: "0"/"1"/"S"/"N" -> BOOLEAN
with source as (
    select * from {{ source('raw', 'produtos') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by produto_id
            order by _ingestion_date desc
        ) as rn
    from source
    where produto_id is not null
),

cleaned as (
    select
        produto_id,
        nullif(trim(nome_produto), '') as nome_produto,
        nullif(trim(categoria), '') as categoria,
        case
            -- formato brasileiro "1.234,56": vírgula como decimal
            when regexp_contains(preco_tabela, r',\d{2}$')
                then replace(regexp_replace(preco_tabela, r'[^0-9,]', ''), ',', '.')
            -- formato "R$ 533.46" / "765.78": ponto como decimal
            else regexp_replace(preco_tabela, r'[^0-9.]', '')
        end as preco_limpo,
        case
            when lower(trim(ativo)) in ('1', 's', 'sim', 'true') then true
            when lower(trim(ativo)) in ('0', 'n', 'nao', 'não', 'false') then false
            else null
        end as ativo
    from deduplicated
    where rn = 1
)

select
    produto_id,
    nome_produto,
    categoria,
    safe_cast(preco_limpo as numeric) as preco_tabela,
    ativo
from cleaned
