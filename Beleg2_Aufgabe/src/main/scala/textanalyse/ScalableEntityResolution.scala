package textanalyse

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkContext}
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.xy.XYSplineRenderer
import org.jfree.chart.{ChartPanel, JFreeChart}
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}
import org.jfree.ui.ApplicationFrame


class ScalableEntityResolution(sc: SparkContext, dat1: String, dat2: String, stopwordsFile: String, goldStandardFile: String) extends EntityResolution(sc, dat1, dat2, stopwordsFile, goldStandardFile) {

  // Creation of the tf-idf-Dictionaries
  createCorpus
  calculateIDF
  val idfsFullBroadcast = sc.broadcast(idfDict)

  // Preparation of all Document Vectors
  def calculateDocumentVector(productTokens: RDD[(String, List[String])], idfDictBroad: Broadcast[Map[String, Double]]): RDD[(String, Map[String, Double])] = {

    productTokens.map(x => (x._1, ScalableEntityResolution.calculateTF_IDFBroadcast(x._2, idfDictBroad)))
  }

  val amazonWeightsRDD: RDD[(String, Map[String, Double])] = calculateDocumentVector(amazonTokens, idfsFullBroadcast)
  //  (b000jz4hqo,Map(premier -> 9.27070707070707, broderbund -> 22.169082125603865, image -> 3.6948470209339774, pack -> 2.98180636777128, rom -> 2.4051362683438153, dvd -> 1.287598204264871, 950 -> 254.94444444444443, 000 -> 6.218157181571815, clickart -> 56.65432098765432))
  val googleWeightsRDD: RDD[(String, Map[String, Double])] = calculateDocumentVector(googleTokens, idfsFullBroadcast)

  // Calculation of the L2-Norms for each Vector
  val amazonNorms = amazonWeightsRDD.map(x => (x._1, EntityResolution.calculateNorm(x._2))).collectAsMap().toMap
  val amazonNormsBroadcast = sc.broadcast(amazonNorms)
  val googleNorms = googleWeightsRDD.map(x => (x._1, EntityResolution.calculateNorm(x._2))).collectAsMap().toMap
  val googleNormsBroadcast = sc.broadcast(googleNorms)


  val BINS = 101
  val nthresholds = 100
  val zeros: Vector[Int] = Vector.fill(BINS) {
    0
  }
  val thresholds = for (i <- 1 to nthresholds) yield i / nthresholds.toDouble
  var falseposDict: Map[Double, Long] = _
  var falsenegDict: Map[Double, Long] = _
  var trueposDict: Map[Double, Long] = _

  var fpCounts = sc.accumulator(zeros)(VectorAccumulatorParam)

  var amazonInvPairsRDD: RDD[(String, String)] = _
  var googleInvPairsRDD: RDD[(String, String)] = _

  var commonTokens: RDD[((String, String), Iterable[String])] = _
  var similaritiesFullRDD: RDD[((String, String), Double)] = _
  var simsFullValuesRDD: RDD[Double] = _
  var trueDupSimsRDD: RDD[Double] = _


  var amazonWeightsBroadcast: Broadcast[Map[String, Map[String, Double]]] = _
  var googleWeightsBroadcast: Broadcast[Map[String, Map[String, Double]]] = _
  this.amazonWeightsBroadcast = amazonWeightsRDD.sparkContext.broadcast(amazonWeightsRDD.collectAsMap().toMap)
  this.googleWeightsBroadcast = amazonWeightsRDD.sparkContext.broadcast(googleWeightsRDD.collectAsMap().toMap)

  def buildInverseIndex: Unit = {

    /*
     * Aufbau eines inversen Index 
     * Die Funktion soll die Variablen amazonWeightsRDD und googleWeightsRDD so
     * umwandeln, dass aus dem EingabeRDD vom Typ  RDD[(String, Map[String,Double])]
     * alle Tupel der Form (Wort, ProduktID) extrahiert werden.
     * Verwenden Sie dazu die Funktion invert im object und speichern Sie die
     * Ergebnisse in amazonInvPairsRDD und googleInvPairsRDD. Cachen Sie die 
     * die Werte.
     */


    //   amazonWeightsRDD = (b000jz4hqo,Map(premier -> 9.27070707070707, broderbund -> 22.169082125603865, image -> 3.6948470209339774, pack -> 2.98180636777128, rom -> 2.4051362683438153, dvd -> 1.287598204264871, 950 -> 254.94444444444443, 000 -> 6.218157181571815, clickart -> 56.65432098765432))
    val amazonWeightsRDD_ = amazonWeightsRDD
    val googleWeightsRDD_ = googleWeightsRDD
    //    val wordCountAmazon = amazonWeightsRDD.flatMap(_._2).map(x => (x, 1.0)).reduceByKey(_+_)
    val temp = amazonWeightsRDD_.flatMap(x => ScalableEntityResolution.invert(x))
    val temp2 = googleWeightsRDD_.flatMap(x => ScalableEntityResolution.invert(x))
    this.amazonInvPairsRDD = temp.cache()
    this.googleInvPairsRDD = temp2.cache()

    //    this.googleInvPairsRDD =
  }


  def determineCommonTokens: Unit = {

    /*
     * Bestimmen Sie alle Produktkombinationen, die gemeinsame Tokens besitzen
     * Speichern Sie das Ergebnis in die Variable commonTokens und verwenden Sie
     * dazu die Funktion swap aus dem object.
     */
    //    def swap(el:(String,(String,String))):((String, String),String)={
    //      /*
    //       * Wandelt das Format eines Elements für die Anwendung der
    //       * RDD-Operationen.
    //       */
    //      (el._2,el._1)
    //    }

    //    (b00005lzly, http://www.google.com/base/feeds/snippets/13823221823254120257)

    buildInverseIndex
    val amazonInvPairsRDD_ = amazonInvPairsRDD
    //    (000,b000jz4hqo)
    val googleInvPairsRDD_ = googleInvPairsRDD
    val temp = amazonInvPairsRDD_.join(googleInvPairsRDD_)
    //    (vga,(b000099sin,http://www.google.com/base/feeds/snippets/12536674159579454939))
    this.commonTokens = temp.map(x => ScalableEntityResolution.swap(x)).groupByKey().cache()
    //    ((b000bta4kg,http://www.google.com/base/feeds/snippets/17451370028015177558),CompactBuffer(easy, premium))
  }

  def calculateSimilaritiesFullDataset: Unit = {

    /*
     * Berechnung der Similarity Werte des gesmamten Datasets 
     * Verwenden Sie dafür das commonTokensRDD (es muss also mind. ein
     * gleiches Wort vorkommen, damit der Wert berechnent dafür.
     * Benutzen Sie außerdem die Broadcast-Variablen für die L2-Norms sowie
     * die TF-IDF-Werte.
     * 
     * Für die Berechnung der Cosinus-Similarity verwenden Sie die Funktion
     * fastCosinusSimilarity im object
     * Speichern Sie das Ergebnis in der Variable similaritiesFullRDD und cachen sie diese.
     */

    determineCommonTokens

    val amazonWeightsBroadcast_ = amazonWeightsBroadcast
    val googleWeightsBroadcast_ = googleWeightsBroadcast
    val amazonNormsBroadcast_ = amazonNormsBroadcast
    val googleNormsBroadcast_ = googleNormsBroadcast

    this.similaritiesFullRDD = commonTokens.map(x => ScalableEntityResolution.fastCosinusSimilarity(x, amazonWeightsBroadcast_, googleWeightsBroadcast_, amazonNormsBroadcast_, googleNormsBroadcast_)).cache()
    this.simsFullValuesRDD = similaritiesFullRDD.values.cache()
  }

  /*
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * 
   * Analyse des gesamten Datensatzes mittels des Gold-Standards
   * 
   * Berechnung:
   * True-Positive
   * False_Positive
   * True-Negative 
   * False-Negative
   * 
   * und daraus
   * Precision
   * Recall 
   * F-Measure
   * 
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   */

  def analyseDataset: Unit = {

    //simsFullValuesRDD
    val simsFullRDD = similaritiesFullRDD.map(x => (x._1._1 + " " + x._1._2, x._2)).cache
    this simsFullValuesRDD = simsFullRDD.values
    simsFullRDD.take(10).foreach(println)
    goldStandard.take(10).foreach(println)
    //val tds=goldStandard.leftOuterJoin(simsFullRDD)
    //tds.filter(x=>x._2._2==None).take(10000).foreach(println)
    trueDupSimsRDD = goldStandard.leftOuterJoin(simsFullRDD).map(ScalableEntityResolution.gs_value(_)).cache()


    def calculateFpCounts(fpCounts: Accumulator[Vector[Int]]): Accumulator[Vector[Int]] = {

      val BINS = this.BINS
      val nthresholds = this.nthresholds
      val fpCounts_ : Accumulator[Vector[Int]] = fpCounts
      simsFullValuesRDD.foreach(ScalableEntityResolution.add_element(_, BINS, nthresholds, fpCounts_))
      trueDupSimsRDD.foreach(ScalableEntityResolution.sub_element(_, BINS, nthresholds, fpCounts_))
      fpCounts_
    }

    fpCounts = calculateFpCounts(fpCounts)
    falseposDict = (for (t <- thresholds) yield (t, falsepos(t, fpCounts))).toMap
    falsenegDict = (for (t <- thresholds) yield (t, falseneg(t))).toMap
    trueposDict = (for (t <- thresholds) yield (t, truepos(t))).toMap

    val precisions = for (t <- thresholds) yield (t, precision(t))
    val recalls = for (t <- thresholds) yield (t, recall(t))
    val fmeasures = for (t <- thresholds) yield (t, fmeasure(t))

    val series1: XYSeries = new XYSeries("Precision");
    for (el <- precisions) {
      series1.add(el._1, el._2)
    }
    val series2: XYSeries = new XYSeries("Recall");
    for (el <- recalls) {
      series2.add(el._1, el._2)
    }
    val series3: XYSeries = new XYSeries("F-Measure");
    for (el <- fmeasures) {
      series3.add(el._1, el._2)
    }

    val datasetColl: XYSeriesCollection = new XYSeriesCollection
    datasetColl.addSeries(series1)
    datasetColl.addSeries(series2)
    datasetColl.addSeries(series3)

    val spline: XYSplineRenderer = new XYSplineRenderer();
    spline.setPrecision(10);

    val xax: NumberAxis = new NumberAxis("Similarities");
    val yax: NumberAxis = new NumberAxis("Precision/Recall/F-Measure");

    val plot: XYPlot = new XYPlot(datasetColl, xax, yax, spline);

    val chart: JFreeChart = new JFreeChart(plot);
    val frame: ApplicationFrame = new ApplicationFrame("Dataset Analysis");
    val chartPanel1: ChartPanel = new ChartPanel(chart);

    frame.setContentPane(chartPanel1);
    frame.pack();
    frame.setVisible(true);
    println("Please press enter....")
//    System.in.read()
  }


  /*
   * Berechnung von False-Positives, FalseNegatives und
   * True-Positives
   */
  def falsepos(threshold: Double, fpCounts: Accumulator[Vector[Int]]): Long = {
    val fpList = fpCounts.value
    (for (b <- Range(0, BINS) if b.toDouble / nthresholds >= threshold) yield fpList(b)).sum
  }

  def falseneg(threshold: Double): Long = {

    trueDupSimsRDD.filter(_ < threshold).count()
  }

  def truepos(threshold: Double): Long = {

    trueDupSimsRDD.count() - falsenegDict(threshold)
  }

  /* 
   * 
   * Precision = true-positives / (true-positives + false-positives)
   * Recall = true-positives / (true-positives + false-negatives)
   * F-measure = 2 x Recall x Precision / (Recall + Precision) 
   */


  def precision(threshold: Double): Double = {
    val tp = trueposDict(threshold)
    tp.toDouble / (tp + falseposDict(threshold))
  }

  def recall(threshold: Double): Double = {
    val tp = trueposDict(threshold)
    tp.toDouble / (tp + falsenegDict(threshold))
  }

  def fmeasure(threshold: Double): Double = {
    val r = recall(threshold)
    val p = precision(threshold)
    2 * r * p / (r + p)
  }
}

object ScalableEntityResolution {

  def calculateTF_IDFBroadcast(terms: List[String], idfDictBroadcast: Broadcast[Map[String, Double]]): Map[String, Double] = {

    /* 
     * Berechnung von TF-IDF Wert für eine Liste von Wörtern
     * Ergebnis ist eine Map die auf jedes Wort den zugehörigen TF-IDF-Wert mapped
     */
    val tF = terms.groupBy(identity).mapValues(_.size.toDouble / terms.size.toDouble)
    tF.map { case (k, v) => (k, v * idfDictBroadcast.value.getOrElse(k, 0.0)) }
  }

  def invert(termlist: (String, Map[String, Double])): List[(String, String)] = {

    //in: List of (ID, tokenList with TFIDF-value)
    //out: List[(token,ID)]

    termlist._2.keys.map(x => (x, termlist._1)).toList
  }

  def swap(el: (String, (String, String))): ((String, String), String) = {

    /*
     * Wandelt das Format eines Elements für die Anwendung der
     * RDD-Operationen.
     */

    (el._2, el._1)
  }

  def fastCosinusSimilarity(record: ((String, String), Iterable[String]),
                            amazonWeightsBroad: Broadcast[Map[String, Map[String, Double]]], googleWeightsBroad: Broadcast[Map[String, Map[String, Double]]],
                            amazonNormsBroad: Broadcast[Map[String, Double]], googleNormsBroad: Broadcast[Map[String, Double]]): ((String, String), Double) = {

    /* Compute Cosine Similarity using Broadcast variables
    Args:
        record: ((ID, URL), token)
    Returns:
        pair: ((ID, URL), cosine similarity value)
        
    Verwenden Sie die Broadcast-Variablen und verwenden Sie für ein schnelles dot-Product nur die TF-IDF-Werte,
    die auch in der gemeinsamen Token-Liste sind 
    */
    //   amazonWeightsRDD = (b000jz4hqo,Map(premier -> 9.27070707070707, broderbund -> 22.169082125603865, image -> 3.6948470209339774, pack -> 2.98180636777128, rom -> 2.4051362683438153, dvd -> 1.287598204264871, 950 -> 254.94444444444443, 000 -> 6.218157181571815, clickart -> 56.65432098765432))
    //      record = ((b000bcz8ng,http://www.google.com/base/feeds/snippets/17911312393691300802),CompactBuffer(4, superior))((.........)).....

    //      amazonWeightsBroad.value.getOrElse(record._1._1, Map()) = Map(ftp -> 24.940217391304348, ws_ftp -> 99.76086956521739, 9 -> 1.5230667109193494, sdk -> 79.80869565217391, inc -> 1.7656791073489801, ipswitch -> 66.5072463768116, wd -> 66.5072463768116, w -> 5.783238815374921, pro -> 1.9231011000523834, 0900 -> 66.5072463768116, ws -> 66.5072463768116, 1000 -> 6.880059970014992) .........

    val cartesianProd = record._2.map(x => amazonWeightsBroad.value.getOrElse(record._1._1, Map()).getOrElse(x, 0.0)
      * googleWeightsBroad.value.getOrElse(record._1._2, Map()).getOrElse(x, 0.0)).sum
    val norm = amazonNormsBroad.value.getOrElse(record._1._1, 0.0) * googleNormsBroad.value.getOrElse(record._1._2, 0.0)

    (record._1, cartesianProd / norm)

  }

  def gs_value(record: (_, (_, Option[Double]))): Double = {

    record._2._2 match {
      case Some(d: Double) => d
      case None => 0.0
    }
  }

  def set_bit(x: Int, value: Int, length: Int): Vector[Int] = {

    Vector.tabulate(length) { i => {
      if (i == x) value else 0
    }
    }
  }

  def bin(similarity: Double, nthresholds: Int): Int = (similarity * nthresholds).toInt

  def add_element(score: Double, BINS: Int, nthresholds: Int, fpCounts: Accumulator[Vector[Int]]): Unit = {
    val b = bin(score, nthresholds)
    fpCounts += set_bit(b, 1, BINS)
  }


  def sub_element(score: Double, BINS: Int, nthresholds: Int, fpCounts: Accumulator[Vector[Int]]): Unit = {
    val b = bin(score, nthresholds)
    fpCounts += set_bit(b, -1, BINS)
  }
}