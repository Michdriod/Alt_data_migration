{% macro render_cast(expr, target_type) %}
    {%- set tt = (target_type or '') | trim -%}
    {%- if tt -%}
        ({{ expr }})::{{ tt | lower }}
    {%- else -%}
        {{ expr }}
    {%- endif -%}
{% endmacro %}

{% macro _apply_null_handling(expr, null_handling, base_alias, source_column) %}
    {%- set nh = (null_handling or '') | trim -%}
    {%- if nh == 'blank_to_null' -%}
        case
            when nullif({{ base_alias }}.{{ source_column | lower }}::text, '') is null then null
            else ({{ expr }})
        end
    {%- elif nh == 'set_null_if_corporate' -%}
        case when upper(coalesce({{ base_alias }}.cust_type, '')) = 'C' then null else ({{ expr }}) end
    {%- elif nh == 'set_null_if_individual' -%}
        case when upper(coalesce({{ base_alias }}.cust_type, '')) = 'I' then null else ({{ expr }}) end
    {%- elif nh == 'keep_blank' -%}
        {{ expr }}
    {%- elif nh == 'error_if_blank' -%}
        /* error_if_blank not enforced at SQL-time; rely on tests */ {{ expr }}
    {%- else -%}
        {{ expr }}
    {%- endif -%}
{% endmacro %}

{% macro _expr_for_row(row, base_alias) %}
    {%- set src_col = row['source_column'] -%}
    {%- set tgt_col = row['target_column'] -%}
    {%- set tgt_type = row['target_data_type'] -%}
    {%- set rule_type = (row['rule_type'] or '') | trim -%}
    {%- set args_json = (row['rule_args'] or '{}') -%}
    {%- set nh = (row['null_handling'] or '') | trim -%}

    {# Base expression from source #}
    {%- set base_expr = base_alias ~ '.' ~ (src_col | lower) -%}

    {# Build expression per rule #}
    {%- if rule_type == 'direct_map' -%}
        {%- set e = base_expr -%}
        {# optional strip spaces #}
        {%- set e = "case when (('" ~ args_json ~ "')::json ->> 'strip_spaces')::boolean then regexp_replace(" ~ e ~ ", '\\s+', '', 'g') else " ~ e ~ " end" -%}
        {# optional trim #}
        {%- set e = "case when (('" ~ args_json ~ "')::json ->> 'trim')::boolean then btrim(" ~ e ~ ") else " ~ e ~ " end" -%}
        {# optional case transform #}
        {%- set e = "case when (('" ~ args_json ~ "')::json ->> 'case') = 'title' then initcap(" ~ e ~ ") when (('" ~ args_json ~ "')::json ->> 'case') = 'upper' then upper(" ~ e ~ ") when (('" ~ args_json ~ "')::json ->> 'case') = 'lower' then lower(" ~ e ~ ") else " ~ e ~ " end" -%}
        {# optional email validation #}
        {%- set e = "case when (('" ~ args_json ~ "')::json ->> 'validate_contains_at')::boolean then case when position('@' in coalesce(" ~ e ~ ", '')) > 0 then " ~ e ~ " else null end else " ~ e ~ " end" -%}
        {%- set e = _apply_null_handling(e, nh, base_alias, src_col) -%}
        {{ return(render_cast(e, tgt_type) ~ ' as ' ~ tgt_col) }}

    {%- elif rule_type == 'map_values' -%}
        {%- set key_expr = "coalesce(" ~ base_expr ~ "::text, '')" -%}
        {%- set e = "coalesce((('" ~ args_json ~ "')::json ->> " ~ key_expr ~ "), (('" ~ args_json ~ "')::json ->> 'default'))" -%}
        {%- set e = _apply_null_handling(e, nh, base_alias, src_col) -%}
        {{ return(render_cast(e, tgt_type) ~ ' as ' ~ tgt_col) }}

    {%- elif rule_type == 'parse_date' -%}
        {%- set e = "case when nullif(" ~ base_expr ~ ", '') is null then null else to_date(" ~ base_expr ~ ", coalesce((('" ~ args_json ~ "')::json ->> 'format'), 'YYYY-MM-DD')) end" -%}
        {{ return(render_cast(e, tgt_type) ~ ' as ' ~ tgt_col) }}

    {%- elif rule_type == 'cast_decimal' -%}
        {%- set e = '(' ~ base_expr ~ ')::numeric(38,2)' -%}
        {%- set e = _apply_null_handling(e, nh, base_alias, src_col) -%}
        {{ return(render_cast(e, tgt_type) ~ ' as ' ~ tgt_col) }}

    {%- elif rule_type == 'derive_band_from_score' -%}
        {%- set e -%}
        case
            when {{ base_expr }} is null then null
            when {{ base_expr }} < 35 then 'LOW'
            when {{ base_expr }} < 70 then 'MEDIUM'
            else 'HIGH'
        end
        {%- endset -%}
        {{ return(render_cast(e, tgt_type) ~ ' as ' ~ tgt_col) }}

    {%- elif rule_type == 'convert_kobo_to_naira' -%}
        {%- set e = '(' ~ base_expr ~ ' / 100.0)::numeric(38,2)' -%}
        {%- set e = _apply_null_handling(e, nh, base_alias, src_col) -%}
        {{ return(render_cast(e, tgt_type) ~ ' as ' ~ tgt_col) }}

    {%- elif rule_type == 'derive_monthly_income' -%}
        {%- set e = '(' ~ base_expr ~ ' / 100.0 / 12.0)::numeric(38,2)' -%}
        {%- set e = _apply_null_handling(e, nh, base_alias, src_col) -%}
        {{ return(render_cast(e, tgt_type) ~ ' as ' ~ tgt_col) }}

    {%- elif rule_type == 'prefix_value' -%}
        {%- set e = "(coalesce((('" ~ args_json ~ "')::json ->> 'prefix'), '') || " ~ base_expr ~ ")" -%}
        {%- set e = _apply_null_handling(e, nh, base_alias, src_col) -%}
        {{ return(render_cast(e, tgt_type) ~ ' as ' ~ tgt_col) }}

    {%- elif rule_type == 'split_name_part' -%}
        {%- set part_name = "(('" ~ args_json ~ "')::json ->> 'part')" -%}
        {%- set part_idx = "case when " ~ part_name ~ " = 'first' then 1 when " ~ part_name ~ " = 'middle' then 2 else 3 end" -%}
        {%- set core = "nullif(split_part(coalesce(" ~ base_expr ~ ", ''), ' ', " ~ part_idx ~ "), '')" -%}
        {%- set only_if = "(('" ~ args_json ~ "')::json ->> 'only_if_customer_type')" -%}
        {%- set e = "case when " ~ only_if ~ " = 'INDIVIDUAL' then case when upper(coalesce(" ~ base_alias ~ ".cust_type, '')) = 'I' then " ~ core ~ " end when " ~ only_if ~ " = 'CORPORATE' then case when upper(coalesce(" ~ base_alias ~ ".cust_type, '')) = 'C' then " ~ core ~ " end else " ~ core ~ " end" -%}
        {%- set e = "case when (('" ~ args_json ~ "')::json ->> 'case') = 'title' then initcap(" ~ e ~ ") else " ~ e ~ " end" -%}
        {{ return(render_cast(e, tgt_type) ~ ' as ' ~ tgt_col) }}

    {%- elif rule_type == 'legal_name_if_corporate' -%}
        {%- set e -%}
        case when upper(coalesce({{ base_alias }}.cust_type, '')) = 'C' then initcap({{ base_expr }}) end
        {%- endset -%}
        {{ return(render_cast(e, tgt_type) ~ ' as ' ~ tgt_col) }}

    {%- else -%}
        {{ return(render_cast(base_expr, tgt_type) ~ ' as ' ~ tgt_col) }}
    {%- endif -%}
{% endmacro %}

{% macro generate_select_from_mapping(entity, target_table, base_alias='base') %}
    {%- set seed_name = var('field_mapping_seed', 'customer_field_level_mappings') -%}
    {%- set rel = ref(seed_name) -%}
    {%- set sql -%}
                select entity, source_column, target_column, target_data_type, rule_type, rule_args, null_handling, is_unique, not_null
                from {{ rel }}
                where entity = '{{ entity }}'
                    and lower(target_table) = '{{ target_table | lower }}'
    {%- endset -%}
    {%- set tbl = run_query(sql) -%}

    {%- set projections = [] -%}
    {%- if execute and tbl -%}
        {%- set names = tbl.column_names -%}
        {%- set idx_source_column = names.index('source_column') -%}
        {%- set idx_target_column = names.index('target_column') -%}
        {%- set idx_target_type = names.index('target_data_type') -%}
        {%- set idx_rule_type = names.index('rule_type') -%}
        {%- set idx_rule_args = names.index('rule_args') -%}
        {%- set idx_null_handling = names.index('null_handling') -%}
        {%- for r in tbl.rows -%}
            {%- set row = {} -%}
            {%- do row.update({'source_column': r[idx_source_column]}) -%}
            {%- do row.update({'target_column': r[idx_target_column]}) -%}
            {%- do row.update({'target_data_type': r[idx_target_type]}) -%}
            {%- do row.update({'rule_type': r[idx_rule_type]}) -%}
            {%- do row.update({'rule_args': r[idx_rule_args]}) -%}
            {%- do row.update({'null_handling': r[idx_null_handling]}) -%}
            {%- set expr = _expr_for_row(row, base_alias) -%}
            {%- do projections.append(expr) -%}
        {%- endfor -%}
    {%- endif -%}

    {{ return(projections | join(',\n\t')) }}
{% endmacro %}
