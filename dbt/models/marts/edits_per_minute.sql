{#
  Incremental: each run recomputes the last 20 minutes, which covers the stream's 10-minute
  watermark plus commit lag, so late events still land in the right minute.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['event_minute', 'wiki'],
) }}

select
    event_minute,
    wiki,
    count(*) as edits,
    count(*) filter (where is_bot) as bot_edits,
    count(*) filter (where not is_bot) as human_edits,
    count(*) filter (where event_type = 'new') as new_pages,
    sum(greatest(length_delta, 0)) as bytes_added,
    sum(greatest(-length_delta, 0)) as bytes_removed
from {{ ref('stg_edits') }}
{% if is_incremental() %}
where event_minute >= (select max(event_minute) from {{ this }}) - interval 20 minutes
{% endif %}
group by event_minute, wiki
