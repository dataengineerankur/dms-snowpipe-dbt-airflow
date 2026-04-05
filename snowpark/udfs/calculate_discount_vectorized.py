from snowflake.snowpark.functions import pandas_udf
from snowflake.snowpark.types import FloatType, StringType, PandasDataFrameType, PandasSeriesType

def register_calculate_discount_vectorized(session):
    return pandas_udf(
        lambda df: (df[0].fillna(0) * df[1].map({'bronze': 0.05, 'silver': 0.10, 'gold': 0.15, 'platinum': 0.20}).fillna(0)).astype(float),
        name='calculate_discount_vectorized',
        return_type=PandasSeriesType(FloatType()),
        input_types=[PandasDataFrameType([FloatType(), StringType()])],
        replace=True,
        session=session,
        packages=['pandas'],
    )
