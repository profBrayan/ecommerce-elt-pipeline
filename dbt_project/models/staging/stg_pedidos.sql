-- Pedidos vindos da API de vendas. Dedup por pedido_id (reruns podem re-extrair
-- o mesmo pedido em dias diferentes); mantém a versão mais recente por updated_at.
with source as (
    select * from {{ source('raw', 'pedidos') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by pedido_id
            order by _ingestion_date desc, updated_at desc
        ) as rn
    from source
    where pedido_id is not null
)

select
    pedido_id,
    cliente_id,
    produto_id,
    quantidade,
    -- valor_unitario vem sujo na origem ("462.99 BRL" além do formato numérico
    -- puro) e por isso é armazenado como STRING na raw; aqui removemos
    -- qualquer coisa que não seja dígito ou ponto antes do cast.
    safe_cast(regexp_replace(valor_unitario, r'[^0-9.]', '') as numeric) as valor_unitario,
    lower(nullif(trim(status), '')) as status,
    safe_cast(data_pedido as timestamp) as data_pedido,
    safe_cast(updated_at as timestamp) as updated_at
from deduplicated
where rn = 1
