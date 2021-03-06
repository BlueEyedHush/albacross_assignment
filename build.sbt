val sparkVersion = "2.0.0"

lazy val prj = (project in file(".")).
  settings(
    organization := "knawara.albacross",
    name := "event_labeler",
    version := "0.1",
    scalaVersion := "2.11.8",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.scalatest" %% "scalatest" % "3.0.0" % "test"
    )
  )
