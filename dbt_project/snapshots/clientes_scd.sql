{% snapshot clientes_scd %}

{{
    config(
        target_schema=target.schema,
        unique_key='cliente_id',
        strategy='check',
        check_cols=['nome', 'cidade', 'estado', 'segmento', 'email'],
    )
}}

select * from {{ ref('stg_clientes') }}

{% endsnapshot %}
