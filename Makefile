spark-submit-local:
	cp ./target/spark-scala-practice-*.jar /opt/spark/jars/
	export SPARK_DIST_CLASSPATH='/opt/spark/conf:/opt/hadoop/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/yarn:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*:/opt/hadoop/share/hadoop/tools/lib/*:/opt/spark/jars/*'; \
	/opt/spark/bin/spark-submit \
	  --verbose \
      --master local[*] \
      --deploy-mode client \
      --name my-example-app \
      --class ru.mnegodaev.Application \
      --conf source_path=some_source_path \
      --conf product_table_name=data_product \
      local:///opt/spark/jars/spark-scala-practice-1.0-SNAPSHOT.jar