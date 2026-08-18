-- Preço médio interno vs. preço médio da concorrência, por categoria.
-- Junta por categoria (não por produto): os nomes de produto do scraping não
-- compartilham chave/identidade com o catálogo interno (dado sintético do
-- case), então um join produto-a-produto seria artificial. Comparar por
-- categoria é a granularidade que a informação disponível realmente sustenta.
with interno as (
    select
        categoria,
        avg(preco_tabela) as preco_medio_interno
    from {{ ref('dim_produtos') }}
    where categoria is not null
    group by categoria
),

concorrencia as (
    select
        categoria,
        avg(preco) as preco_medio_concorrencia
    from {{ ref('stg_precos_concorrentes') }}
    where categoria is not null
    group by categoria
)

select
    coalesce(interno.categoria, concorrencia.categoria) as categoria,
    interno.preco_medio_interno,
    concorrencia.preco_medio_concorrencia,
    round(
        safe_divide(
            interno.preco_medio_interno - concorrencia.preco_medio_concorrencia,
            concorrencia.preco_medio_concorrencia
        ) * 100,
        2
    ) as gap_percentual
from interno
full outer join concorrencia using (categoria)
