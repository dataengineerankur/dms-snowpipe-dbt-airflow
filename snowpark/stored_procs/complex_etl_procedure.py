from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, current_timestamp, lit, when
from snowflake.snowpark.types import StringType

def register_complex_etl_procedure(session: Session) -> None:
    def complex_etl_procedure(session: Session, batch_date: str) -> str:
        new_orders = session.table('RAW_DB.PUBLIC.RAW_ORDERS').filter(col('ORDER_DATE').cast('date') == lit(batch_date))
        row_count = new_orders.count()
        if row_count == 0:
            return f'No new orders for {batch_date}'
        scored = new_orders.with_column('RISK_SCORE', when(col('AMOUNT') > 5000, lit(0.9)).when(col('AMOUNT') > 1000, lit(0.5)).otherwise(lit(0.1))).with_column('PROCESSED_AT', current_timestamp())
        scored.write.mode('append').save_as_table('SILVER_DB.PUBLIC.SCORED_ORDERS')
        return f'Processed {row_count} orders for {batch_date}'

    session.sproc.register(
        func=complex_etl_procedure,
        name='RUN_COMPLEX_ETL',
        packages=['snowflake-snowpark-python'],
        return_type=StringType(),
        input_types=[StringType()],
        replace=True,
        is_permanent=False,
    )
