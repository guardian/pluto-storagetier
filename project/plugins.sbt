logLevel := Level.Warn

resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.22")

// for snyk
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")