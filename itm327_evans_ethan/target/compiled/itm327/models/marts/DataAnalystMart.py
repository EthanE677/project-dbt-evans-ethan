# show stock data alongside news data

from snowflake.snowpark.functions import (
    col, avg, stddev, max as max_, min as min_,
    count, sum as sum_, lit, when, to_date
)


def model(dbt, session):
    """
    Snowpark Python model:
    one row per stock symbol with both trading + news stats.
    """

    # =========================
    # STOCK DATA
    # =========================
    stocks_df = dbt.ref('raw_stocks')

    stocks_with_metrics = stocks_df.withColumn(
        'DAILY_RANGE', col('HIGH') - col('LOW')
    ).withColumn(
        'DAILY_CHANGE', col('CLOSE') - col('OPEN')
    ).withColumn(
        'DAILY_RETURN_PCT', (col('CLOSE') - col('OPEN')) / col('OPEN') * lit(100)
    )

    stock_summary = (
        stocks_with_metrics
        .groupBy('SYMBOL')
        .agg(
            count('TRADE_DATE').alias('TRADING_DAYS'),
            min_('TRADE_DATE').alias('FIRST_TRADE_DATE'),
            max_('TRADE_DATE').alias('LAST_TRADE_DATE'),
            max_('HIGH').alias('PERIOD_HIGH'),
            min_('LOW').alias('PERIOD_LOW'),
            avg('CLOSE').alias('AVG_CLOSE'),
            avg('VOLUME').alias('AVG_DAILY_VOLUME'),
            sum_('VOLUME').alias('TOTAL_VOLUME'),
            avg('DAILY_RANGE').alias('AVG_DAILY_RANGE'),
            stddev('DAILY_RETURN_PCT').alias('RETURN_VOLATILITY'),
            avg('DAILY_RETURN_PCT').alias('AVG_DAILY_RETURN_PCT'),
            sum_(when(col('DAILY_CHANGE') > lit(0), lit(1)).otherwise(lit(0))).alias('UP_DAYS'),
            sum_(when(col('DAILY_CHANGE') < lit(0), lit(1)).otherwise(lit(0))).alias('DOWN_DAYS'),
        )
    )

    # =========================
    # NEWS DATA 
    # =========================
    news_df = dbt.ref('raw_news')

    news_with_date = news_df.withColumn(
        'NEWS_DATE', to_date(col('DATETIME'))
    )

    news_summary = (
        news_with_date
        .groupBy('RELATED')
        .agg(
            count('*').alias('TOTAL_ARTICLES'),
            count('NEWS_DATE').alias('ARTICLE_DAYS')
        )
    )

    # =========================
    # JOIN DATA
    # =========================
    final_df = (
        stock_summary
        .join(
            news_summary,
            stock_summary['SYMBOL'] == news_summary['RELATED'],
            how='left'
        )
        .drop('RELATED')  # avoid duplicate column
    )

    return final_df


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"raw_news": "SNOWBEARAIR_DB.RAW.raw_news", "raw_stocks": "SNOWBEARAIR_DB.RAW.raw_stocks"}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "SNOWBEARAIR_DB"
    schema = "RAW"
    identifier = "DataAnalystMart"
    
    def __repr__(self):
        return 'SNOWBEARAIR_DB.RAW.DataAnalystMart'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


