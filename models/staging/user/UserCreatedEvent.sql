{{ config(materialized='incremental', unique_key =  '"EventId"') }}
{{ create_stage_table('UserCreatedEvent') }}