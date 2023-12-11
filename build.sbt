name := "skyline"

version := "0.1"

scalaVersion := "2.13.12"

resolvers += "Maven Central" at "https://repo.maven.apache.org/maven2/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0"
)