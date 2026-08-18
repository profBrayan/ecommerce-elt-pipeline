-- Versão corrente de cada cliente, a partir do snapshot SCD2.
select
    cliente_id,
    nome,
    cpf,
    email,
    cidade,
    estado,
    segmento,
    data_cadastro,
    dbt_valid_from as cliente_valido_desde
from {{ ref('clientes_scd') }}
where dbt_valid_to is null
