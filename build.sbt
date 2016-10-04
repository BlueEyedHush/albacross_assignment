lazy val prj = (project in file(".")).
  settings(
    organization := "knawara.albacross",
    name := "event-labeler",
    version := "0.1",
    scalaVersion := "2.12.0-RC1"
  )
