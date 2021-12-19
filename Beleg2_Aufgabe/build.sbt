name := "SimilarityAnalysisAufgabe"
version := "0.1"

scalaVersion := "2.12.7"

run := Defaults.runTask(fullClasspath in Runtime, mainClass in run in Compile, runner in run).evaluated

parallelExecution in Test := false

libraryDependencies ++=Seq("org.apache.spark" %% "spark-core" % "2.4.5",
			   "org.apache.spark" %% "spark-sql" % "2.4.5",
			   "org.jfree" % "jfreechart" % "1.0.19",
			   "org.scalatest" %% "scalatest" % "3.1.1" % "test",
			   "org.scalactic" %% "scalactic" % "3.1.1")




