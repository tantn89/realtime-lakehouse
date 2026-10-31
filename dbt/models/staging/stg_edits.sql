-- Edits with the columns the marts use. Content namespace 0 is the article space.
select
    event_id,
    event_time,
    date_trunc('minute', event_time) as event_minute,
    cast(event_time as date) as event_date,
    event_type,
    wiki,
    namespace,
    namespace = 0 as is_article,
    title,
    "user" as editor,
    is_bot,
    coalesce(length_delta, 0) as length_delta,
    processed_at
from {{ source('silver', 'edits') }}
