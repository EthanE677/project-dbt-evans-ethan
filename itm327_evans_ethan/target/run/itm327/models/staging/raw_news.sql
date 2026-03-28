
  create or replace   view SNOWBEARAIR_DB.RAW.raw_news
  
   as (
    -- TODO: Update the source table name to match your prefix (e.g., SMITHJ_NEWS)
select *
from SNOWBEARAIR_DB.RAW.SFTP_API_EVANS_E
  );

