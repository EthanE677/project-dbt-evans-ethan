-- TODO: Update the source table name to match your prefix (e.g., SMITHJ_STOCKS)
select *, CAST(DATETIME AS DATE) AS TRADE_DATE
from SNOWBEARAIR_DB.RAW.STONKS