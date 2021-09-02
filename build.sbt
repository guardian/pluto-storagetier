import com.typesafe.sbt.packager.docker
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport.{dockerExposedPorts, dockerUsername}
import com.typesafe.sbt.packager.docker.{Cmd, DockerPermissionStrategy}
import sbt.Keys.scalacOptions

name := "pluto-storagetier"



javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
scalacOptions += "-target:jvm-1.8"

val akkaVersion = "2.6.16"
val circeVersion = "0.14.1"
val slf4jVersion = "1.7.32"
val elastic4sVersion = "6.7.8"
val sttpVersion = "1.7.2"

lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.13.6",
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
  scalacOptions += "-target:jvm-1.8"
)


lazy val `common` = (project in file("common"))
  .settings(commonSettings,
    Docker / aggregate := false,
    Docker / publish := {},
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.specs2" %% "specs2-core" % "4.12.3" % Test,
      "org.specs2" %% "specs2-mock" % "4.12.3" % Test,
      "org.mockito" %% "mockito-scala-specs2" % "1.16.39" % Test
    )
  )

lazy val `online_archive` = (project in file("online_archive"))
  .enablePlugins(DockerPlugin, AshScriptPlugin)
  .dependsOn(common)
  .settings(commonSettings,
    version := sys.props.getOrElse("build.number","DEV"),
    dockerPermissionStrategy := DockerPermissionStrategy.CopyChown,
    Docker / daemonUserUid := None,
    Docker / daemonUser := "daemon",
    Docker / dockerUsername  := sys.props.get("docker.username"),
    Docker / dockerRepository := Some("guardianmultimedia"),
    Docker / packageName := "guardianmultimedia/storagetier-online-archive",
    packageName := "mediacensus",
    dockerBaseImage := "openjdk:11-jdk-alpine",
    dockerAlias := docker.DockerAlias(None,Some("guardianmultimedia"),"storagetier-online-archive",Some(sys.props.getOrElse("build.number","DEV"))),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "org.scala-lang.modules" %% "scala-xml" % "2.0.1",
      "org.specs2" %% "specs2-core" % "4.12.3" % Test,
      "org.specs2" %% "specs2-mock" % "4.12.3" % Test,
      "org.mockito" %% "mockito-scala-specs2" % "1.16.39" % Test
    )
  )
