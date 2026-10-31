-- The incremental mart must agree exactly with silver for every closed minute: a mismatch
-- means a late event fell outside the recompute window or a run was skipped.
-- Minutes newer than 25 minutes may still be receiving late events and are not checked.
with cutoff as (
    select max(event_minute) - interval 25 minutes as closed_before from {{ ref('edits_per_minute') }}
),
silver as (
    select event_minute, wiki, count(*) as edits
    from {{ ref('stg_edits') }}
    where event_minute < (select closed_before from cutoff)
    group by event_minute, wiki
),
mart as (
    select event_minute, wiki, edits
    from {{ ref('edits_per_minute') }}
    where event_minute < (select closed_before from cutoff)
)
select
    coalesce(s.event_minute, m.event_minute) as event_minute,
    coalesce(s.wiki, m.wiki) as wiki,
    s.edits as silver_edits,
    m.edits as mart_edits
from silver s
full outer join mart m using (event_minute, wiki)
where s.edits is distinct from m.edits
