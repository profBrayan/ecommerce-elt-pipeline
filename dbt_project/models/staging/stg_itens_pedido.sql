-- Modelo incremental: os 5M+ itens_pedido não são reprocessados inteiramente a
-- cada run. `raw.itens_pedido` é recarregado por partição de ingestão (dia) na
-- camada raw; aqui só processamos as partições ainda não vistas por este
-- modelo (is_incremental), e o merge por item_id garante idempotência mesmo
-- que o mesmo dia seja reprocessado.
{{
    config(
        materialized='incremental',
        unique_key='item_id',
        incremental_strategy='merge',
        partition_by={'field': 'data_item', 'data_type': 'timestamp'},
        cluster_by=['cliente_id', 'produto_id'],
    )
}}

with source as (
    select * from {{ source('raw', 'itens_pedido') }}
    {% if is_incremental() %}
    where _ingestion_date > (select coalesce(max(_ingestion_date), date('1900-01-01')) from {{ this }})
    {% endif %}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by item_id
            order by _ingestion_date desc
        ) as rn
    from source
    where item_id is not null
)

select
    item_id,
    pedido_id,
    cliente_id,
    produto_id,
    safe_cast(data_item as timestamp) as data_item,
    quantidade,
    safe_cast(valor_unitario as numeric) as valor_unitario,
    _ingestion_date
from deduplicated
where rn = 1
