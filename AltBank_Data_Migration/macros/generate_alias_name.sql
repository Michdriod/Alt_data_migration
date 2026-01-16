{# 
  Macro: generate_alias_name
  Purpose: Standardize relation names per layer, idempotently.
    - staging → stg_<model_name>
    - transformation → xform_<model_name>
    - final → <model_name> (no prefix)

  Inputs:
    - custom_alias_name: explicit alias via {{ config(alias='...') }}; if provided, it wins.
    - node: the dbt graph node; used to read model name and folder (via fqn).

  Notes:
    - Idempotent prefixing: if the file is already named with stg_/xform_, we do not double-prefix.
    - Folder detection relies on 'staging', 'transformation', 'final' in node.fqn.
#}
{% macro generate_alias_name(custom_alias_name=none, node=none) %}
  {# Respect an explicit alias set in the model config #}
  {% if custom_alias_name is not none %}
    {{ return(custom_alias_name) }}
  {% endif %}

  {# Derive base name from the model file, and inspect folder segments #}
  {% set name = node.name %}
  {% set parts = node.fqn[1:] %}  {# skip project name in fqn #}

  {# Staging: ensure single 'stg_' prefix #}
  {% if 'staging' in parts %}
    {% if name.startswith('stg_') %}
      {{ return(name) }}
    {% else %}
      {{ return('stg_' ~ name) }}
    {% endif %}

  {# Transformation: ensure single 'xform_' prefix #}
  {% elif 'transformation' in parts %}
    {% if name.startswith('xform_') %}
      {{ return(name) }}
    {% else %}
      {{ return('xform_' ~ name) }}
    {% endif %}

  {# Final: no prefix; use the base name #}
  {% elif 'final' in parts %}
    {{ return(name) }}

  {# Fallback: leave name unchanged #}
  {% else %}
    {{ return(name) }}
  {% endif %}
{% endmacro %}