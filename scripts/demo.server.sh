#!/bin/bash


#Enter the log dir
cd $1

#tables=(view addtocart)
tables=(view)
for table in "${tables[@]}"
do
    echo "Working on table $table ..."
    # clean db
    echo "truncate \"$table\"" | hbase shell
    status=$?
    if [ $status -ne 0 ]; then
      echo "################################################################################################################"
      echo "                                The previous command may have failed."
      echo "################################################################################################################"
    fi
    #Load in Hbase
    $HBASE_HOME/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=, -Dimporttsv.columns=HBASE_ROW_KEY,u:id,p:id,c:id $table demo.$table.csv
done

#Run Spark Job
$SPARK_HOME/bin/spark-submit --class "com.yreco.RecommenderDemoWithServer" --master local[8] /home/yawo/Studio/ytech/others/finagle/target/scala-2.11/yreco-assembly-1.0.jar
