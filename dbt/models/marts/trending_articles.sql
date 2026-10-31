-- Articles drawing the most human attention in the last hour of data: ranked by distinct
-- human editors first (one prolific editor is not a trend), then by edit count.
with recent as (
    select *
    from {{ ref('stg_edits') }}
    where is_article
      and not is_bot
      and event_time > (select max(event_time) from {{ ref('stg_edits') }}) - interval 1 hour
)

select
    wiki,
    title,
    count(distinct editor) as editors,
    count(*) as edits,
    max(event_time) as last_edit_at,
    row_number() over (order by count(distinct editor) desc, count(*) desc, max(event_time) desc)
        as trend_rank
from recent
group by wiki, title
qualify trend_rank <= 50
