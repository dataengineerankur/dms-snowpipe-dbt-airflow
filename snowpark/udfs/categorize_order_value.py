from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import FloatType, StringType

def register_categorize_order_value(session):
    @udf(name='categorize_order_value', return_type=StringType(), input_types=[FloatType()], replace=True, session=session)
    def categorize_order_value(amount):
        amount = amount or 0.0
        if amount < 50:
            return 'micro'
        if amount < 200:
            return 'small'
        if amount < 1000:
            return 'medium'
        return 'large'
    return categorize_order_value
