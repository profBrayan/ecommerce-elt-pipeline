-- Funil de pedidos da API de vendas: volume e receita por dia/status.
select
    date(data_pedido) as data,
    status,
    count(*) as total_pedidos,
    sum(quantidade) as unidades,
    round(sum(quantidade * valor_unitario), 2) as receita_total
from {{ ref('stg_pedidos') }}
group by data, status
