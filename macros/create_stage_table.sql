{% macro create_stage_table(event_name) %}

{% set metadata_query %}  
	SELECT 
		'SELECT "EventName" , "EventCreated", "EventExternalReferenceId", "EventId", "EventLoadedByPipeTimestamp", ' ||
			ARRAY_TO_STRING(
			ARRAY_AGG(
				'"EventData":"' || EVENT_ATTRIBUTE || '"::' ||
				CASE
					WHEN NUMBER_PREC IS NULL
					THEN DATATYPE
					ELSE CONCAT(DATATYPE, '(', NUMBER_PREC, ',', COALESCE(NUMBER_SCALE, 0), ')')
				END ||  ' AS "' || TARGET_COLUMN || '" '
			) WITHIN GROUP(
				ORDER BY
					EVENT_ATTRIBUTE),
			',')||
			'FROM  (
				SELECT
					"EventName",
					"EventCreated",
					"EventExternalReferenceId",
					"EventId",
					"EventLoadedByPipeTimestamp",
					PARSE_JSON("EventData") "EventData"
				FROM {{ source('staging', 'EventDump') }} ed
				WHERE ed."EventName" = ''{{event_name}}''
				{% if is_incremental() %}
				AND "EventLoadedByPipeTimestamp" > (SELECT MAX("EventLoadedByPipeTimestamp") FROM {{this}})
				{% endif %}
			)'	  AS "query_to_get_new_events"
	FROM
		{{ ref('seed_EventContract') }}
	WHERE
		EVENT_NAME = '{{event_name}}'
	GROUP BY
		EVENT_NAME
{%- endset -%}

{% if execute %}
  {%- set query_result = run_query(metadata_query).columns['query_to_get_new_events'].values()[0] -%}
{% else %}
  {%- set query_result = '' -%}
{% endif %}

{{return(query_result)}}
{% endmacro %}