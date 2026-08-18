select
    produto_id,
    nome_produto,
    categoria,
    preco_tabela,
    ativo
from {{ ref('stg_produtos') }}
