-- Falha se algum item de pedido tiver quantidade ou valor_unitario negativos
-- (dado sujo proposital pode conter isso; queremos saber, não silenciar).
select item_id, quantidade, valor_unitario
from {{ ref('stg_itens_pedido') }}
where quantidade < 0 or valor_unitario < 0
