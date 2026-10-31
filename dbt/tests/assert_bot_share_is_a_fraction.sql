select event_date, wiki, bot_share
from {{ ref('bot_activity_daily') }}
where bot_share < 0 or bot_share > 1
