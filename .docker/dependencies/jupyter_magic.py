import pyspark
from delta import *
from IPython.core.magic import (register_line_magic, register_cell_magic,
register_line_cell_magic)
import ipywidgets as w
from itables import init_notebook_mode
import itables.options as opt
init_notebook_mode(all_interactive=True)
opt.style = "table-layout:auto;width:auto;float:left"


#Magics

@register_cell_magic
def sql(cell,query=None):
    from datetime import datetime

    if not query.lower().startswith("select"):
        spark.sql(str(query))
    else:
        now = datetime.now()
        now = now.strftime("%Y%m%d%H%M%S")
        toggle = w.ToggleButton(description='Download',
                            layout=w.Layout(width='100px')
                            )
        out = w.Output(layout=w.Layout(border = '1px solid black'))
        table_query = spark.sql(str(query)).limit(1000).toPandas().head()
        def fun(obj):
            with out:
                spark.sql(str(query)).toPandas().head().to_csv(f'./{now}_data.csv', index=False)
                out.clear_output()
        display(table_query)
        toggle.observe(fun, 'value')
        display(toggle)
        display(out)

#Spark
#builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
#    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#spark = configure_spark_with_delta_pip(builder).getOrCreate()