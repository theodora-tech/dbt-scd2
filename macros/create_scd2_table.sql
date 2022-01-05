{## CREATE SCD TYPE 2 TABLE
  # Arguments:
  #   dim_cols - dict - Description of dimensions
  #     {
  #       'col_1': { 'evt_1': 'col_1', 'evt_2': 'col_1' },
  #       'col_2': { 'evt_2': 'col_2', 'evt_1': 'col_2_alt' },
  #       'col_3': { 'evt_2': 'col_3' }
  #     }
  #   business_key_attr - str - Business key to partition by
  #   surrogate_key_name - str - name of surrogate key column
##}
{%- macro hash_event_names(event_names) %}
  {%- set acronyms = [] %}
  {%- for event_name in event_names %}
    {%- set _ = acronyms.append(event_name|select('upper')|join) %}
  {%- endfor %}
  {{-acronyms|sort|join("_")-}}
{%- endmacro %}

{%- macro get_bk_alias(cte_alias, business_key_attr) %}
  {{-"%s_%s"|format(cte_alias, business_key_attr|replace('"', ''))-}}
{%- endmacro %}

{%- macro create_scd2_table(dim_cols, business_key_attr, surrogate_key_name) %}
{# Get distinct event names #}
{%- set event_names = [] %}
{%- for dim_col, evt_src in dim_cols.items() %}
  {%- for event_name in evt_src.keys() %}
    {%- set _ = event_names.append(event_name) %}
  {%- endfor %}
{%- endfor %}
{%- set event_names = event_names|unique|list %}

{# Get event sources for distinct CTEs necessary to build dims #}
{## Example output
{
  "event1_event2": {
    "dim1": {
      "event1": "dim1_src_col",
      "event2": "dim1_src_col"
    },
    "dim3": {
      "event1": "dim3_src_col",
      "event2": "dim3_src_col_with_funny_name"
    }
  },
  "event1": {
    "dim2": {
      "event1": "dim2_src_col"
    }
  }
}
##}
{%- set cte_col_sources = {} %}
{%- set cte_event_sources = {} %}
{%- for dim_col in dim_cols.keys()|select('ne', business_key_attr) %}{# business_key_attr not needed as a sub-CTE #}
  {%- set evt_src = dim_cols[dim_col] %}

  {%- set src_event_names = evt_src.keys() %}
  {%- set src_hash = hash_event_names(src_event_names) %}
  {%- if src_hash not in cte_event_sources.keys() %}
    {%- set _ = cte_event_sources.update({src_hash: src_event_names|list})%}
  {%- endif %}
  {%- if src_hash not in cte_col_sources.keys() %}
    {%- set _ = cte_col_sources.update({src_hash: {dim_col: evt_src}}) %}
  {%- else %}
    {%- set _ = cte_col_sources[src_hash].update({dim_col: evt_src}) %}
  {%- endif %}
{%- endfor %}

{# Render CTEs #}
WITH
{%- for hash, cols in cte_col_sources.items() %}
src_{{hash}} AS
(
  SELECT
    *,
    LEAD("EventCreated") OVER (PARTITION BY {{get_bk_alias(hash, business_key_attr)}} ORDER BY "EventCreated") AS "NextEventCreated"
  FROM
  (
    {%- for evt_name in cte_event_sources[hash] %}
    {%- set src_business_key = dim_cols[business_key_attr][evt_name] %}
    SELECT
      {{src_business_key}} AS {{get_bk_alias(hash, business_key_attr)}},
      "EventCreated"
      {%- for dim_col_name, evt_attr in cols.items() %}
        ,{{cols[dim_col_name][evt_name]}} AS {{dim_col_name}}
      {%- endfor %}
    FROM {{ ref(evt_name) }}
    {%- if not loop.last %}
    UNION
    {%- endif %}
    {%- endfor %}
  )
),
{% endfor %}
tot AS
(
  {%- for evt_name in event_names %}
  SELECT
    "EventId",
    "EventName",
    "EventCreated",
    {%- for dim_col, evt_src in dim_cols.items() %}
      {{'NULL' if evt_name not in evt_src else evt_src[evt_name] }} AS {{ dim_col }}{%- if not loop.last %},{%- endif %}
    {%- endfor %}
  FROM {{ ref(evt_name) }}
  {%- if not loop.last %}
  UNION
  {%- endif %}
  {%- endfor %}
)

{# Render SELECT #}
SELECT
  {{ dbt_utils.surrogate_key(['tot."EventId"', 'tot."EventName"', 'tot."EventCreated"']) }} AS {{surrogate_key_name}},
  tot."EventId",
  tot."EventName",
  tot."EventCreated" "RecordCreated",
  LEAD(tot."EventCreated") OVER (PARTITION BY {{ business_key_attr }} ORDER BY tot."EventCreated") AS "RecordModified",
  "RecordModified" IS NULL AS "IsCurrent",
  tot.{{business_key_attr}},
{%- for dim_col in dim_cols.keys()|select('ne', business_key_attr) %}{# Handled above (tot.{{business_key_attr}}) #}
  {%- set evt_src = dim_cols[dim_col] %}
  CASE WHEN tot."EventName" IN ({%- for evt in evt_src.keys() %}'{{evt}}'{%- if not loop.last %},{%- endif %}{%- endfor %})
    THEN tot.{{dim_col}}
    ELSE src_{{hash_event_names(evt_src.keys())}}.{{dim_col}}
  END AS {{dim_col}}{%- if not loop.last %},{%- endif %}
{%- endfor %}
FROM tot
{%- for hash, cols in cte_col_sources.items() %}
{%- set src = 'src_' + hash %}
LEFT JOIN {{src}} ON tot.{{business_key_attr}} = {{src}}.{{get_bk_alias(hash, business_key_attr)}} AND tot."EventCreated" > {{src}}."EventCreated" AND ({{src}}."NextEventCreated" IS NULL OR tot."EventCreated" <= {{src}}."NextEventCreated")
{%- endfor %}
{%- if is_incremental() %}
WHERE tot.{{business_key_attr}} IN
(
  SELECT {{business_key_attr}} FROM tot WHERE "EventCreated" > (SELECT MAX("RecordCreated") FROM {{this}})
)
{%- endif %}

{%- endmacro %}
