-- How much of each wiki's daily activity comes from bots.
select
    event_date,
    wiki,
    count(*) as edits,
    round(avg(case when is_bot then 1.0 else 0.0 end), 4) as bot_share,
    count(distinct editor) filter (where not is_bot) as human_editors,
    count(distinct editor) filter (where is_bot) as bot_accounts
from {{ ref('stg_edits') }}
group by event_date, wiki
