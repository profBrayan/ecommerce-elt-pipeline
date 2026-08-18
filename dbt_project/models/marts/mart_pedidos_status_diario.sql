-- Funil de pedidos da API de vendas: volume e receita por dia/status.
-- Alguns pedidos vêm da origem sem data_pedido (dado sujo proposital da API).
-- Um grão diário não tem onde colocar um pedido sem data, então esses casos
-- são excluídos aqui (mas continuam visíveis em stg_pedidos, nada é perdido
-- antes da staging) em vez de gerar uma linha com data nula na série temporal.
select
    date(data_pedido) as data,
    status,
    count(*) as total_pedidos,
    sum(quantidade) as unidades,
    round(sum(quantidade * valor_unitario), 2) as receita_total
from {{ ref('stg_pedidos') }}
where data_pedido is not null
group by data, status
