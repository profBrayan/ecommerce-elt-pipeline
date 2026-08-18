-- Alerta (não falha o build, ver severity em config) quando o mesmo CPF
-- aparece em mais de um cliente_id — indica duplicidade de cadastro que a
-- dedup por cliente_id sozinha não resolve. Documentado como limitação
-- conhecida no README.
{{ config(severity='warn') }}

select cpf, count(distinct cliente_id) as clientes_distintos
from {{ ref('stg_clientes') }}
where cpf is not null
group by cpf
having count(distinct cliente_id) > 1
