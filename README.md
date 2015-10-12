# yreco
![Hybris](http://static1.squarespace.com/static/52b0f50ae4b0bb29db95a077/t/547f6d4ee4b046b3148da91c/1417637198924/hybris-icon.png)

![Spark](http://spark.apache.org/docs/latest/img/spark-logo-hd.png)

![Finagle](https://pbs.twimg.com/profile_images/378800000650818162/903f3158869844f68b0fd36dde6b2d29_400x400.png)

![Thrift](http://no-cache.appdynamics-static.com/appsphere/logos/thrift_350.png)

![Amazon](http://1.bp.blogspot.com/-SJhlctsnGFA/UFYEW8c5VvI/AAAAAAAAACk/-ZBbpmv886s/s1600/20100517_amazon.gif)

![Bash](http://img.bfmtv.com/c/630/420/617/df99c0369426d0dd1b2d20f5c4ff4.jpg)

![SpringIntegration](http://jpalace.org/store/docs/tutorials/spring/integration/img/eip-adapter.png)

Apache Spark Machine Learning application applied to Hybris business events.

## Built with:

* Apache thrift,
* Twitter Finagle
* Apache Hbase
* Apache Spark MLLib (ALS algorithm)
* Hybris (business events,Hotfolders, Product References, ...)
* Spring integration
* Unix Bash 

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
         
       
