-- `categoria` é extraída de forma best-effort (ver parsing.py) e pode vir nula
-- quando a estrutura da página não expõe essa dica no momento do scraping —
-- isso é esperado e tratado no mart (agregação despreza nulos).
select
    produto,
    loja,
    nullif(trim(categoria), '') as categoria,
    preco,
    estoque_status,
    _ingestion_date,
    _ingestion_run_id
from {{ source('raw', 'precos_concorrentes') }}
where preco is not null
