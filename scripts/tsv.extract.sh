#!/bin/sh

#Enter the log dir
cd $1
#Split
grep AddToCartEvent             events.tsv             | sed -e "s/\([^,]*,\)\{2\}\([^,]*\),[^,]*,\([^,]*\),\([^,]*,\)\{3\}\([^,]*\),\([^,]*,\)\{5\}\([^,]*\).*/\2_\3_\5_\7,\3,\5,\7/g" >> addtocart.tsv
grep ProductDetailPageViewEvent events.tsv             | sed -e "s/\([^,]*,\)\{2\}\([^,]*\),[^,]*,\([^,]*\),\([^,]*,\)\{3\}\([^,]*\),\([^,]*,\)\{5\}\([^,]*\).*/\2_\3_\5_\7,\3,\5,\7/g" >> view.tsv
#grep ProductDetailPageViewEvent events.tsv | sed -e 's/\([^,]*,\)\{2\}\([^,]*\),[^,]*,\([^,]*\),\([^,]*,\)\{3\}\([^,]*\),\([^,]*,\)\{5\}\([^,]*\).*/\2,\3,\5,\7/g' >> view.tsv

#Load in Hbase
$HBASE_HOME/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=, -Dimporttsv.columns=HBASE_ROW_KEY,u:id,p:id,c:id addtocart addtocart.tsv
$HBASE_HOME/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=, -Dimporttsv.columns=HBASE_ROW_KEY,u:id,p:id,c:id view      view.tsv
#Run ML on Spark and generate /tmp/sims.csv and /tmp/recos.csv for the view
$SPARK_HOME/bin/spark-submit --class "com.yreco.RecommenderDemoBatch" --master local[8] /home/yawo/Studio/ytech/others/finagle/target/scala-2.11/yreco-assembly-1.0.jar


#Archive files
mkdir -p archives
mv events.tsv archives/events.$(date +'%Y%m%d%H%M%S').tsv
rm addtocart.tsv view.tsv

