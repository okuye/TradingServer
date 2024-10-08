ThisBuild / scalaVersion     := "3.3.0"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "TradingServer",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"          % "2.0.15",
      "dev.zio" %% "zio-json"     % "0.5.0",
      "dev.zio" %% "zio-http"     % "3.0.0-RC2",
      "dev.zio" %% "zio-streams"  % "2.0.15",
      "dev.zio" %% "zio-test"     % "2.0.15" % Test,
      "dev.zio" %% "zio-test-sbt" % "2.0.15" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )