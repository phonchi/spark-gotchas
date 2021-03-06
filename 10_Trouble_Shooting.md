## Spark java.lang.OutOfMemoryError: Java heap space

from http://stackoverflow.com/questions/21138751/spark-java-lang-outofmemoryerror-java-heap-space

- If your nodes are configured to have 6g maximum for Spark (and are leaving a little for other processes), then use 6g rather than 4g, spark.executor.memory=6g. Make sure you're using as much memory as possible by checking the UI (it will say how much mem you're using)
- Try using more partitions, you should have 2 - 4 per CPU. IME increasing the number of partitions is often the easiest way to make a program more stable (and often faster). For huge amounts of data you may need way more than 4 per CPU, I've had to use 8000 partitions in some cases!
- Decrease the fraction of memory reserved for caching, using spark.storage.memoryFraction. If you don't use cache() or persist in your code, this might as well be 0. It's default is 0.6, which means you only get 0.4 * 4g memory for your heap. IME reducing the mem frac often makes OOMs go away. UPDATE: From spark 1.6 apparently we will no longer need to play with these values, spark will determine them automatically.
- Similar to above but shuffle memory fraction. If your job doesn't need much shuffle memory then set it to a lower value (this might cause your shuffles to spill to disk which can have catastrophic impact on speed). Sometimes when it's a shuffle operation that's OOMing you need to do the opposite i.e. set it to something large, like 0.8, or make sure you allow your shuffles to spill to disk (it's the default since 1.0.0).
- Watch out for memory leaks, these are often caused by accidentally closing over objects you don't need in your lambdas. The way to diagnose is to look out for the "task serialized as XXX bytes" in the logs, if XXX is larger than a few k or more than an MB, you may have a memory leak. See http://stackoverflow.com/a/25270600/1586965
- Related to above; use broadcast variables if you really do need large objects.
- If you are caching large RDDs and can sacrifice some access time consider serialising the RDD http://spark.apache.org/docs/latest/tuning.html#serialized-rdd-storage. Or even caching them on disk (which sometimes isn't that bad if using SSDs).
- (Advanced) Related to above, avoid String and heavily nested structures (like Map and nested case classes). If possible try to only use primitive types and index all non-primitives especially if you expect a lot of duplicates. Choose WrappedArray over nested structures whenever possible. Or even roll out your own serialisation - YOU will have the most information regarding how to efficiently back your data into bytes, USE IT!
- (bit hacky) Again when caching, consider using a Dataset to cache your structure as it will use more efficient serialisation. This should be regarded as a hack when compared to the previous bullet point. Building your domain knowledge into your algo/serialisation can minimise memory/cache-space by 100x or 1000x, whereas all a Dataset will likely give is 2x - 5x in memory and 10x compressed (parquet) on disk.

## Task-not-serializable-java-io-notserializable exception-when-calling-function
form http://stackoverflow.com/questions/21138751/spark-java-lang-outofmemoryerror-java-heap-space and
https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/troubleshooting/javaionotserializableexception.html

## Missing Dependencies in Jar Files (shaded, uber jar)
https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/troubleshooting/missing_dependencies_in_jar_files.html


