package textanalyse

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class EntityResolution(sc: SparkContext, dat1: String, dat2: String, stopwordsFile: String, goldStandardFile: String) {

  //  (b000jz4hqo,clickart 950 000 - premier image pack (dvd-rom)  "broderbund")
  val amazonRDD: RDD[(String, String)] = Utils.getData(dat1, sc)
  val googleRDD: RDD[(String, String)] = Utils.getData(dat2, sc)
  val stopWords: Set[String] = Utils.getStopWords(stopwordsFile)

  //  for calculateDocumentSimilarity()
  //val tempRDD1:RDD[(String, String)]= Utils.getData(dat1, sc)

  val goldStandard: RDD[(String, String)] = Utils.getGoldStandard(goldStandardFile, sc)

  var amazonTokens: RDD[(String, List[String])] = _
  var googleTokens: RDD[(String, List[String])] = _
  var corpusRDD: RDD[(String, List[String])] = _
  var idfDict: Map[String, Double] = _

  var idfBroadcast: Broadcast[Map[String, Double]] = _
  var similarities: RDD[(String, String, Double)] = _

  def getTokens(data: RDD[(String, String)]): RDD[(String, List[String])] = {

    /*
     * getTokens soll die Funktion tokenize auf das gesamte RDD anwenden
     * und aus allen Produktdaten eines RDDs die Tokens extrahieren.
     */
    val stopwords_ = this.stopWords
    data.map(x => (x._1, EntityResolution.tokenize(x._2, stopwords_))).cache()
  }

  def countTokens(data: RDD[(String, List[String])]): Long = {

    /*
     * Zählt alle Tokens innerhalb eines RDDs
     * Duplikate sollen dabei nicht eliminiert werden
     */
    data.map(x => x._2.length).reduce((a, b) => a + b)
  }

  def findBiggestRecord(data: RDD[(String, List[String])]): (String, List[String]) = {
    def maxFun(record1: ((String, List[String]), Int), record2: ((String, List[String]), Int)): ((String, List[String]), Int) = {
      if (record1._2 > record2._2) record1 else record2
    }
    /*
     * Findet den Datensatz mit den meisten Tokens
     */
    data.map(x => (x, x._2.length)).reduce((a, b) => maxFun(a, b))._1
  }


  def createCorpus = {

    /*
     * Erstellt die Tokenmenge für die Amazon und die Google-Produkte
     * (amazonRDD und googleRDD), vereinigt diese und speichert das
     * Ergebnis in corpusRDD
     *
     * Bsp. amazonTokens OR corpusRDD = (b000jz4hqo,List(clickart, 950, 000, premier, image, pack, dvd, rom, broderbund))
     */
    amazonTokens = this.getTokens(amazonRDD)
    googleTokens = this.getTokens(googleRDD)
    corpusRDD = sc.union(amazonTokens, googleTokens)
  }


  def calculateIDF = {

    /*
     * Berechnung des IDF-Dictionaries auf Basis des erzeugten Korpus
     * Speichern des Dictionaries in die Variable idfDict
     */
    createCorpus
    val numberOfDocuments = corpusRDD.count().toDouble
    //    val wordCountInCorpus = corpusRDD.flatMap(_._2).map(x => (x, 1.0)).reduceByKey(_ + _)
    val wordsInAmazon = amazonTokens.flatMap(_._2).collect().toList
    val wordsInGoogle = googleTokens.flatMap(_._2).collect().toList

    val wordsInCorpus = corpusRDD.flatMap(_._2).distinct().collect().toSet

    def countWords(word: String): Double = {
      corpusRDD.filter(x => x._2.contains(word)).count.toDouble
    }



    //    val tF = EntityResolution.getTermFrequencies(wordsInAmazon)
    //    idfDict = wordOccurence.map(x => (x._1, x._2*))

    idfDict = wordsInCorpus.map(x => (x, numberOfDocuments / countWords(x))).toMap

  }

  def simpleSimimilarityCalculation: RDD[(String, String, Double)] = {

    /*
     * Berechnung der Document-Similarity für alle möglichen 
     * Produktkombinationen aus dem amazonRDD und dem googleRDD
     * Ergebnis ist ein RDD aus Tripeln bei dem an erster Stelle die AmazonID
     * steht, an zweiter die GoogleID und an dritter der Wert
     */
    calculateIDF
    val idfDict_ = idfDict
    val stopWords_ = stopWords
    val cartesian = amazonRDD.cartesian(googleRDD)
    cartesian.map(x => (x._1._1, x._2._1, EntityResolution.calculateDocumentSimilarity(x._1._2, x._2._2, idfDict_, stopWords_)))
  }

  def findSimilarity(vendorID1: String, vendorID2: String, sim: RDD[(String, String, Double)]): Double = {

    /*
     * Funktion zum Finden des Similarity-Werts für zwei ProduktIDs
     */
    sim.filter(x => x._1 == vendorID1 && x._2 == vendorID2).first()._3
  }

  def simpleSimimilarityCalculationWithBroadcast: RDD[(String, String, Double)] = {

    calculateIDF
    val idfBroadcast_ = sc.broadcast(idfDict)
    val stopWords_ = stopWords
    val cartesian = amazonRDD.cartesian(googleRDD)
    cartesian.map(x => (x._1._1, x._2._1, EntityResolution.computeSimilarityWithBroadcast((x._1, x._2), idfBroadcast_, stopWords_)._3)).cache()
  }

  /*
   *
   * 	Gold Standard Evaluation
   */

  def evaluateModel(goldStandard: RDD[(String, String)]): (Long, Double, Double) = {

    /*
     * Berechnen Sie die folgenden Kennzahlen:
     * 
     * Anzahl der Duplikate im Sample
     * Durchschnittliche Consinus Similaritaet der Duplikate
     * Durchschnittliche Consinus Similaritaet der Nicht-Duplikate
     * 
     * 
     * Ergebnis-Tripel:
     * (AnzDuplikate, avgCosinus-SimilaritätDuplikate,avgCosinus-SimilaritätNicht-Duplikate)
     */
    val goldStandard_ = goldStandard.map(x => (x._1, x._2))
//    (b000jz4hqo http://www.google.com/base/feeds/snippets/18441480711193821750,gold)
    val temp = simpleSimimilarityCalculationWithBroadcast.map(x => (x._1 + " " + x._2, x._3))

    val duplikate = temp.join(goldStandard_)
    val nichtDuplikate = temp.subtractByKey(goldStandard_)
    val AnzDuplikate = duplikate.count()
    val AnzNichtDuplikate = nichtDuplikate.count()
    val avgCosinus_SimilaritätDuplikate = duplikate.map(x => x._2._1).sum()/AnzDuplikate
    val avgCosinus_SimilaritätNicht_Duplikate = nichtDuplikate.map(x => x._2).sum()/AnzNichtDuplikate

    (AnzDuplikate, avgCosinus_SimilaritätDuplikate,avgCosinus_SimilaritätNicht_Duplikate)
  }
}

object EntityResolution {

  def tokenize(s: String, stopws: Set[String]): List[String] = {
    /*
   	* Tokenize splittet einen String in die einzelnen Wörter auf
   	* und entfernt dabei alle Stopwords.
   	* Verwenden Sie zum Aufsplitten die Methode Utils.tokenizeString
   	*/
    val splittedWords = Utils.tokenizeString(s) //:List[String]
    splittedWords.filter(x => !stopws.contains(x))

  }


  def getTermFrequencies(tokens: List[String]): Map[String, Double] = {
    /*
     * Berechnet die Relative Haeufigkeit eine Wortes in Bezug zur
     * Menge aller Wörter innerhalb eines Dokuments 
     */
    tokens.groupBy(identity).mapValues(_.size.toDouble / tokens.size.toDouble)
  }

  def computeSimilarity(record: ((String, String), (String, String)), idfDictionary: Map[String, Double], stopWords: Set[String]): (String, String, Double) = {

    /*
     * Bererechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem 
     * Sie die erforderlichen Parameter extrahieren
     */
    (record._1._1, record._2._1, calculateDocumentSimilarity(record._1._2, record._2._2, idfDictionary, stopWords))
  }

  def calculateTF_IDF(terms: List[String], idfDictionary: Map[String, Double]): Map[String, Double] = {

    /* 
     * Berechnung von TF-IDF Wert für eine Liste von Wörtern
     * Ergebnis ist eine Mapm die auf jedes Wort den zugehörigen TF-IDF-Wert mapped
     */
    val tF = getTermFrequencies(terms)
    tF.map { case (k, v) => (k, v * idfDictionary.getOrElse(k, 0.0)) }
  }

  def calculateDotProduct(v1: Map[String, Double], v2: Map[String, Double]): Double = {

    /*
     * Berechnung des Dot-Products von zwei Vectoren
     */
    //    val updatedV1 = v1.map(x => (x._1.sorted, x._2))
    //    val updatedV2 = v2.map(x => (x._1.sorted, x._2))
    v1.map(x => (x._1, x._2 * v2.getOrElse(x._1, 0.0))).values.sum

    //    }
    //    temp.map(x => if (x._1._1 == x._2._1) x._1._2 * x._2._2 else 0.0).sum
    //    }
    //    val temp = dot.map(x => x._1 * x._2)
  }

  def calculateNorm(vec: Map[String, Double]): Double = {

    /*
     * Berechnung der Norm eines Vectors
     */
    Math.sqrt(((for (el <- vec.values) yield el * el).sum))
  }

  def calculateCosinusSimilarity(doc1: Map[String, Double], doc2: Map[String, Double]): Double = {

    /* 
     * Berechnung der Cosinus-Similarity für zwei Vectoren
     */

    calculateDotProduct(doc1, doc2) / (calculateNorm(doc1) * calculateNorm(doc2))
  }

  def calculateDocumentSimilarity(doc1: String, doc2: String, idfDictionary: Map[String, Double], stopWords: Set[String]): Double = {

    /*
     * Berechnung der Document-Similarity für ein Dokument
     */
    val temp1 = tokenize(doc1, stopWords)
    val temp2 = tokenize(doc2, stopWords)
    val tF_IDF1 = calculateTF_IDF(temp1, idfDictionary)
    val tF_IDF2 = calculateTF_IDF(temp2, idfDictionary)
    calculateCosinusSimilarity(tF_IDF1, tF_IDF2)
  }

  def computeSimilarityWithBroadcast(record: ((String, String), (String, String)), idfBroadcast: Broadcast[Map[String, Double]], stopWords: Set[String]): (String, String, Double) = {

    /*
     * Bererechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem 
     * Sie die erforderlichen Parameter extrahieren
     * Verwenden Sie die Broadcast-Variable.
     *
     * abweichung bei testen :
     */
    (record._1._1, record._2._1, calculateDocumentSimilarity(record._1._2, record._2._2, idfBroadcast.value, stopWords))
  }
}
