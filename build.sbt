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
val slickVersion = "3.3.3"

lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.13.6",
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
  scalacOptions += "-target:jvm-1.8"
)

libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test

Test / testOptions ++= Seq( Tests.Argument("junitxml", "junit.outdir", sys.env.getOrElse("SBT_JUNIT_OUTPUT","/tmp")), Tests.Argument("console") )

lazy val `common` = (project in file("common"))
  .enablePlugins(plugins.JUnitXmlReportPlugin)
  .settings(commonSettings,
    Docker / aggregate := false,
    Docker / publish := {},
    libraryDependencies ++= Seq(
      "com.rabbitmq" % "amqp-client" % "5.13.1",
      "commons-codec" % "commons-codec" % "1.15",
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "com.typesafe.slick" %% "slick" % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp" % slickVersion,
      "org.postgresql" % "postgresql" % "42.2.23",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "ch.qos.logback" % "logback-core" % "1.2.6",
      "ch.qos.logback" % "logback-classic" % "1.2.6",
      "org.specs2" %% "specs2-core" % "4.12.12" % Test,
      "org.specs2" %% "specs2-mock" % "4.12.12" % Test,
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
    dockerBaseImage := "openjdk:11-jdk-slim",
    dockerAlias := docker.DockerAlias(None,Some("guardianmultimedia"),"storagetier-online-archive",Some(sys.props.getOrElse("build.number","DEV"))),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % "10.2.6",
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "org.scala-lang.modules" %% "scala-xml" % "2.0.1",
      "org.specs2" %% "specs2-core" % "4.12.3" % Test,
      "org.specs2" %% "specs2-mock" % "4.12.3" % Test,
      "org.mockito" %% "mockito-scala-specs2" % "1.16.39" % Test,
      "com.amazonaws" % "aws-java-sdk-s3" % "1.11.372"
    )
  )
