name := "yreco"

version := "1.0"

scalaVersion := "2.11.6"

resolvers += "Twitter Repository" at "http://maven.twttr.com"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

resolvers += "Repo at github.com/ankurdave/maven-repo" at "https://github.com/ankurdave/maven-repo/raw/master"


libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-thriftmux" % "6.28.0",
  "com.twitter" %% "scrooge-core" % "4.1.0" % "provided",
  "org.apache.hbase" % "hbase-common" % "1.0.1.1",
  "org.apache.hbase" % "hbase-server" % "1.0.1.1",
  "org.apache.hbase" % "hbase-client" % "1.0.1.1",
  "org.apache.spark" % "spark-mllib_2.11" % "1.5.1" % "provided",
  "amplab" % "spark-indexedrdd" % "0.3"
)

