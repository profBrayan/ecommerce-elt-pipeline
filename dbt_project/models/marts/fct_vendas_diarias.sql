-- Grão: dia x categoria de produto x segmento de cliente.
-- Fonte primária de "vendas" é itens_pedido (5M linhas, a fonte transacional
-- real do SQLite) — não os pedidos da API, que ficam como uma fatia própria
-- em mart_pedidos_status_diario (ver nota no README sobre por que os dois
-- pedido_id não são tratados como o mesmo universo de pedidos).
with itens as (
    select * from {{ ref('stg_itens_pedido') }}
),

produtos as (
    select * from {{ ref('dim_produtos') }}
),

clientes as (
    select * from {{ ref('dim_clientes') }}
),

enriched as (
    select
        date(itens.data_item) as data,
        coalesce(produtos.categoria, 'sem_categoria') as categoria,
        coalesce(clientes.segmento, 'nao_informado') as segmento,
        itens.item_id,
        itens.produto_id,
        itens.quantidade,
        itens.quantidade * itens.valor_unitario as receita_item
    from itens
    left join produtos on itens.produto_id = produtos.produto_id
    left join clientes on itens.cliente_id = clientes.cliente_id
)

select
    data,
    categoria,
    segmento,
    count(distinct produto_id) as produtos_distintos,
    count(item_id) as itens_vendidos,
    sum(quantidade) as unidades_vendidas,
    round(sum(receita_item), 2) as receita_total,
    round(safe_divide(sum(receita_item), sum(quantidade)), 2) as ticket_medio_unidade
from enriched
group by data, categoria, segmento
