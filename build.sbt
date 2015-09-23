name := "finagle"

version := "1.0"

scalaVersion := "2.11.6"

resolvers += "Twitter Repository" at "http://maven.twttr.com"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

resolvers += "Repo at github.com/ankurdave/maven-repo" at "https://github.com/ankurdave/maven-repo/raw/master"


libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-thriftmux" % "6.28.0",
  "com.twitter" %% "scrooge-core" % "3.18.1",
  "org.apache.hbase" % "hbase-common" % "1.0.1.1",
  "org.apache.hbase" % "hbase-server" % "1.0.1.1",
  "org.apache.hbase" % "hbase-client" % "1.0.1.1",
  "org.apache.spark" % "spark-mllib_2.10" % "1.4.0",
  "amplab" % "spark-indexedrdd" % "0.2"
)
