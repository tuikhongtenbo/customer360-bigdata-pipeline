{% materialization view, adapter='synapse' %}
    {%- set target_relation = this.incorporate(type='view') -%}

    -- Create or Alter View directly, avoiding the temp view creation and rename
    -- which is not supported in Azure Synapse Serverless SQL Pool.
    {%- call statement('main') -%}
        create or alter view {{ target_relation.include(database=False) }} as
        {{ sql }}
    {%- endcall -%}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
