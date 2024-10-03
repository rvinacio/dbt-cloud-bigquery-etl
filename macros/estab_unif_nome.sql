{% macro estab_unif_nome(estabelecimento) %}

    {% set tags_estab_unif = run_query(
        "select nome_unificado, tags from dbt_cloud.tags_estabelecimento_unificado order by id_tags_estab_unif"
    ) %}
    case
        {% for row in tags_estab_unif %}
            when regexp_contains({{ estabelecimento }}, r'({{ row.tags }})')
            then '{{ row.nome_unificado }}'
        {% endfor %}
        else initcap({{ estabelecimento }})
    end

{% endmacro %}