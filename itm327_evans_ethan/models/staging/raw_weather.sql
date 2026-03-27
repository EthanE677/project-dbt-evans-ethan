-- TODO: Update the source table name to match your prefix (e.g., SMITHJ_WEATHER)
select *
from {{ source('snowbearair', 'WEATHER_API_EVANS_E') }}