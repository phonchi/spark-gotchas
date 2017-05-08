# PySpark Applications

## Estimate the size of RDD

```
import py4j.protocol  
from py4j.protocol import Py4JJavaError  
from py4j.java_gateway import JavaObject  
from py4j.java_collections import JavaArray, JavaList

from pyspark import RDD, SparkContext  
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer

# your dataframe what you'd estimate
#df

# Helper function to convert python object to Java objects
def _to_java_object_rdd(rdd):  
    """ Return a JavaRDD of Object by unpickling
    It will convert each Python object into Java object by Pyrolite, whenever the
    RDD is serialized in batch or not.
    """
    rdd = rdd._reserialize(AutoBatchedSerializer(PickleSerializer()))
    return rdd.ctx._jvm.org.apache.spark.mllib.api.python.SerDe.pythonToJava(rdd._jrdd, True)

# First you have to convert it to an RDD 
JavaObj = _to_java_object_rdd(df.rdd)

# Now we can run the estimator
sc._jvm.org.apache.spark.util.SizeEstimator.estimate(JavaObj)
```

## Cut lineage for high performance
```
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Row

def cutLineage(df):
    """
    Cut the lineage of a DataFrame - used for iterative algorithms
    .. Note: This uses internal members and may break between versions
    >>> df = rdd.toDF()
    >>> cutDf = cutLineage(df)
    >>> cutDf.count()
    3
    """
    jRDD = df._jdf.toJavaRDD()
    jSchema = df._jdf.schema()
    jRDD.cache()
    sqlCtx = df.sql_ctx
    try:
        javaSqlCtx = sqlCtx._jsqlContext
    except:
        javaSqlCtx = sqlCtx._ssql_ctx
    newJavaDF = javaSqlCtx.createDataFrame(jRDD, jSchema)
    newDF = DataFrame(newJavaDF, sqlCtx)
    return newDF
```
