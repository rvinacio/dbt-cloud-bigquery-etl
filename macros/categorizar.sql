{% macro categorizar(nome_estabelecimento) %}
-- Essa macro estipula a categoria do estabelecimento unificado
    {% set tags_categoria = run_query(
        "select id_categoria, categoria, upper(tags) as tags from dbt_cloud.categoria order by id_sequencia"
    ) %}

    case
        {% for row in tags_categoria %}

            when regexp_contains({{ nome_estabelecimento }}, r'({{ row.tags }})')
            then '{{ row.id_categoria }}'
        {% endfor %}
        else '58c70d59-6581-4bd4-bed5-22d4b669b804'
    end

{% endmacro %}