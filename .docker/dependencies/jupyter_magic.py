import pyspark
from delta import *
from pyspark.sql.functions import col, date_format
from pyspark.sql import DataFrame
from IPython.core.magic import (register_line_magic, register_cell_magic,
register_line_cell_magic)
from functools import partial
import ipywidgets as w
from itables import init_notebook_mode
import itables.options as opt
init_notebook_mode(all_interactive=True)
opt.style = "table-layout:auto;width:auto;float:left"

@register_cell_magic
def sql(cell,query=None):
    query_list = filter_query(query)
    result = list(map(lambda query: query_data(query), query_list))
    last_result = result[-1:][0]
    if isinstance(last_result, DataFrame):
        result_query = last_result.limit(1000).toPandas().head()
        display(result_query)
        download_csv(last_result)


def download_csv(dataframe):
        toggle, out = create_button()
        def fun(obj):
                with out:
                    now = get_time()
                    dataframe.toPandas().to_csv(f'./{now}_data.csv', index=False)
                    out.clear_output()
        toggle.observe(fun, 'value')
        display(toggle)
        display(out)


def filter_query(query):
    query_list = str(query).replace("\n","").split(";")
    query_list = [query.strip() for query in query_list]
    query_list = list(filter(lambda query: query != "", query_list))
    return query_list


def data_wrangling(query):
        table_query = spark.sql(str(query))
        timestamp_cols = [col_name for col_name, col_type in table_query.dtypes if col_type == "timestamp"]
        for col_name in timestamp_cols:
            table_query = table_query.withColumn(col_name, date_format(col(col_name), "yyyy-MM-dd HH:mm:ss"))

        return table_query


def get_time():
        from datetime import datetime
        now = datetime.now()
        now = now.strftime("%Y%m%d%H%M%S")
        return now


def query_data(query):
        if not query.lower().startswith("select"):
                spark.sql(str(query))
        else:
                table_query = data_wrangling(query)
                return table_query


def create_button():
        toggle = w.ToggleButton(description='Download',
                 layout=w.Layout(width='100px')
                 )
        out = w.Output(layout=w.Layout(border = '1px solid black'))
        return toggle, out



# Spark
builder = pyspark.sql.SparkSession.builder.appName("Default") \
   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()