# yreco
![Hybris](http://www.computerworld.in/sites/default/files/news/2013/08/SAP-Outlines-Plans-for-Hybris,-Push-into-'Omni-commerce'_394x296.jpg) ![Spark](http://spark.apache.org/docs/latest/img/spark-logo-hd.png) ![Finagle](https://pbs.twimg.com/profile_images/378800000650818162/903f3158869844f68b0fd36dde6b2d29_400x400.png)
![Amazon](http://1.bp.blogspot.com/-SJhlctsnGFA/UFYEW8c5VvI/AAAAAAAAACk/-ZBbpmv886s/s1600/20100517_amazon.gif)


Apache Spark Machine Learning application applied to Hybris business events.

## Uses:

* Apache thrift,
* Twitter Finagle
* Apache Hbase
* Apache Spark MLLib (ALS algorithm)
* Hybris business events

To provides a real time recommender engine queryable from Hybris.

## Basic Flow:

Hybris Business Events => 
     (Unix sed mini ETL)  => 
       Hbase storage          =>
          Spark MLLib ALS algorithm =>
             ThriftMux Server (Twitter Finagle) =>
               Hybris Client (Twitter Finagle)
               

## Road map:               
  Add Solr to the stack.
         
       
