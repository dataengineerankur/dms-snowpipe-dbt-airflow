"""Snowflake University Snowpark pipelines."""
from __future__ import annotations
import os
import time
from snowflake.snowpark import Session
from snowflake.snowpark.functions import avg, col, lit, when

CONNECTION_PARAMS = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'RAW_DB'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
}

def get_session() -> Session:
    missing = [k for k, v in CONNECTION_PARAMS.items() if not v]
    if missing:
        raise RuntimeError(f'Missing Snowflake connection settings: {missing}')
    return Session.builder.configs(CONNECTION_PARAMS).create()

def timed(label: str, fn):
    start = time.time()
    result = fn()
    print(f'[{label}] elapsed={time.time() - start:.2f}s')
    return result

def run_dataframe_pipeline(session: Session) -> None:
    orders_df = session.table('RAW_DB.PUBLIC.RAW_ORDERS')
    products_df = session.table('RAW_DB.PUBLIC.RAW_PRODUCTS')
    enriched = (
        orders_df.join(products_df, orders_df['PRODUCT_ID'] == products_df['PRODUCT_ID'])
        .select(
            orders_df['ORDER_ID'],
            orders_df['CUSTOMER_ID'],
            orders_df['AMOUNT'],
            products_df['CATEGORY'],
            (orders_df['AMOUNT'] - products_df['COST_PRICE']).alias('GROSS_PROFIT'),
        )
        .filter(col('AMOUNT') > 100)
        .sort(col('AMOUNT').desc())
    )
    enriched.explain()
    print(enriched.queries)
    timed('save_enriched', lambda: enriched.write.mode('overwrite').save_as_table('SILVER_DB.PUBLIC.SP_ORDERS_ENRICHED'))

def run_udf_demo(session: Session) -> None:
    from snowpark.udfs.categorize_order_value import register_categorize_order_value
    from snowpark.udfs.calculate_discount_vectorized import register_calculate_discount_vectorized
    categorize = register_categorize_order_value(session)
    customers_df = session.table('RAW_DB.PUBLIC.RAW_CUSTOMERS').select('CUSTOMER_ID', 'TIER')
    orders_df = session.table('RAW_DB.PUBLIC.RAW_ORDERS').select('ORDER_ID', 'CUSTOMER_ID', 'AMOUNT')
    joined = orders_df.join(customers_df, 'CUSTOMER_ID')

    try:
        calculate_discount = register_calculate_discount_vectorized(session)
        discount_col = calculate_discount(col('AMOUNT'), col('TIER')).alias('DISCOUNT_VALUE')
    except Exception as exc:
        print(f'[vectorized_udf_registration_failed] {exc}')
        discount_col = lit(None).cast('float').alias('DISCOUNT_VALUE')

    result = joined.select(
        col('ORDER_ID'), col('AMOUNT'), col('TIER'),
        categorize(col('AMOUNT')).alias('VALUE_CATEGORY'),
        discount_col,
    )
    timed('write_udf_demo', lambda: result.write.mode('overwrite').save_as_table('SILVER_DB.PUBLIC.SP_ORDER_VALUE_SEGMENTS'))

def run_stored_proc_demo(session: Session) -> None:
    from snowpark.stored_procs.complex_etl_procedure import register_complex_etl_procedure
    register_complex_etl_procedure(session)
    today = session.sql("SELECT TO_CHAR(CURRENT_DATE(), 'YYYY-MM-DD')").collect()[0][0]
    print(session.call('RUN_COMPLEX_ETL', today))

def run_feature_engineering_demo(session: Session) -> None:
    df = session.table('RAW_DB.PUBLIC.RAW_ORDERS').group_by(col('REGION')).agg(
        avg(col('AMOUNT')).alias('AVG_AMOUNT')
    ).select(
        col('REGION'), col('AVG_AMOUNT'),
        when(col('AVG_AMOUNT') > lit(500), lit('HIGH')).otherwise(lit('NORMAL')).alias('SEGMENT')
    )
    timed('feature_table', lambda: df.write.mode('overwrite').save_as_table('GOLD_DB.PUBLIC.SP_REGION_FEATURES'))

def run_lazy_demo(session: Session) -> None:
    orders_df = session.table('RAW_DB.PUBLIC.RAW_ORDERS')
    products_df = session.table('RAW_DB.PUBLIC.RAW_PRODUCTS')
    plan = orders_df.filter(col('AMOUNT') > 100).join(products_df, 'PRODUCT_ID')
    print('Plan built — zero credits used until action')
    plan.explain()
    timed('lazy_collect', lambda: plan.limit(20).collect())

def main() -> None:
    session = get_session()
    run_dataframe_pipeline(session)
    run_udf_demo(session)
    run_stored_proc_demo(session)
    run_feature_engineering_demo(session)
    run_lazy_demo(session)
    session.close()

if __name__ == '__main__':
    main()
