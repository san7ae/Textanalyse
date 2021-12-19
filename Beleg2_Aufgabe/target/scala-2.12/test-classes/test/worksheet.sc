import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD





  val FILENAME_ACCESS_PATTERN = """^(.+)\/(\w+\.\w+)$""".r
  val DATAFILE_PATTERN = """^(.+),"(.+)",(.*),(.*),(.*)""".r
  val GOLDFILE_PATTERN = """^(.+),"(.+)""".r
  val split_regex = "\\W+"


def deleteQuotes(s:String):String=
  for (i <- s if i!='\"') yield i

def parseLine(line:String):(Any,Int)={

  line match {
    case DATAFILE_PATTERN(id,title,description,manufacturer,price) =>
      if (id.equals("\"id\"")) (line,0)
      else
        ((deleteQuotes(id),title+" "+description+" "+manufacturer),1)
    case _ => (line,-1)
  }
}

  def getData(fileName: String, sc: SparkContext): RDD[(String, String)] = {

    val url = getClass.getResource("/" + fileName).getPath
    val path = url match {
      case FILENAME_ACCESS_PATTERN(path, name) => (path, name)
      case _ => ("", "")
    }
    val rawdata = sc.textFile("file://" + url).map(parseLine).cache
    val failed = rawdata.filter(_._2 == -1).cache
    if (failed.count > 0) {
      println("Failed to parse lines:");
      failed.take(10).foreach(println)
    }
    rawdata.filter(_._2 == 1).map(_._1.asInstanceOf[(String, String)]).cache
}

val dat1= "Amazon_small.csv"
var sc:SparkContext=_

val amazonRDD= getData(dat1, sc).take(5)
println(amazonRDD)