-- delete+insert must never leave two rows for the same minute and wiki.
select event_minute, wiki, count(*) as copies
from {{ ref('edits_per_minute') }}
group by event_minute, wiki
having count(*) > 1
