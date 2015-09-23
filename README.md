# yreco
![Hybris](http://www.computerworld.in/sites/default/files/news/2013/08/SAP-Outlines-Plans-for-Hybris,-Push-into-'Omni-commerce'_394x296.jpg)


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
         
       
