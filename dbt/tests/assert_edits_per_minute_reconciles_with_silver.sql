-- The incremental mart must agree exactly with silver for every closed minute: a mismatch
-- means a late event fell outside the recompute window, a run was skipped, or the mart
-- stopped advancing (silver-only minutes show up through the full outer join).
-- The cutoff comes from silver, not the mart, so a stalled or empty mart cannot move it.
-- Minutes newer than 25 minutes may still be receiving late events and are not checked;
-- only the 6 hours before that are, so the test stays cheap as history grows. Both bounds
-- are whole minutes, so filtering silver on event_time (its partition column) is exact.
with cutoff as (
    select least(max(event_minute), date_trunc('minute', current_timestamp))
               - interval 25 minutes as closed_before
    from {{ ref('stg_edits') }}
),
bounds as (
    select closed_before - interval 6 hours as checked_from, closed_before from cutoff
),
silver as (
    select event_minute, wiki, count(*) as edits
    from {{ ref('stg_edits') }}
    where event_time >= (select checked_from from bounds)
      and event_time < (select closed_before from bounds)
    group by event_minute, wiki
),
mart as (
    select event_minute, wiki, edits
    from {{ ref('edits_per_minute') }}
    where event_minute >= (select checked_from from bounds)
      and event_minute < (select closed_before from bounds)
)
select
    coalesce(s.event_minute, m.event_minute) as event_minute,
    coalesce(s.wiki, m.wiki) as wiki,
    s.edits as silver_edits,
    m.edits as mart_edits
from silver s
full outer join mart m using (event_minute, wiki)
where s.edits is distinct from m.edits
