{#
  Incremental: each run recomputes the last 20 minutes, which covers the stream's 10-minute
  watermark plus commit lag, so late events still land in the right minute.
  The window starts from the mart's newest minute, with two guards:
  - an empty mart (first build ran before silver had rows) has no max; fall back to the
    epoch so the next run backfills everything instead of matching nothing forever;
  - one future-dated event must not drag the window ahead of real time, so the anchor is
    capped at the current minute (minute-aligned, so no minute is ever half-recomputed).
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
where event_minute >= least(
    coalesce((select max(event_minute) from {{ this }}), timestamp '1970-01-01'),
    date_trunc('minute', current_timestamp)
) - interval 20 minutes
{% endif %}
group by event_minute, wiki
