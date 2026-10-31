-- Session shape per wiki and editor type: how long people (and bots) edit in one sitting.
select
    cast(session_start as date) as session_date,
    wiki,
    is_bot,
    count(*) as sessions,
    round(avg(edits), 2) as avg_edits_per_session,
    round(median(date_diff('second', session_start, session_end)) / 60.0, 1)
        as median_session_minutes,
    sum(bytes_changed) as bytes_changed
from {{ source('silver', 'editor_sessions') }}
group by session_date, wiki, is_bot
