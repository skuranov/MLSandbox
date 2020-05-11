package ubuntu

import org.apache.spark.mllib.clustering.{LDA, LDAModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.mutable

object LDATraining {

  val ss = startLocalSparkSession

  def startLocalSparkSession = SparkSession.builder()
    .appName("LDA topic modeling")
    .master("local[*]")
    .getOrCreate()

  def createSchema = new StructType()
    .add("folder", StringType, true)
    .add("dialogueID", StringType, true)
    .add("date", StringType, true)
    .add("from", StringType, true)
    .add("to", StringType, true)
    .add("text", StringType, true)

  def getInitialRDD(schema: StructType): Dataset[String] = {
    import ss.implicits._
    ss.read
      .option("header", "true")
      .schema(schema)
      .csv("src/main/resources/dialogueText.csv")
      .map(_.getString(5))
      .filter(_.nonEmpty)
      .persist()
  }

  def loadStopWords(path: String): Set[String] =
    ss.read.text(path)
      .collect().map(row => row.getString(0)).toSet

  def getWords(rawDataset: Dataset[String]): Dataset[Array[String]] = {
    import ss.implicits._
    rawDataset
      .map(
        _.toLowerCase
          .split("\\s"))
      .map(_.filter(_.length > 3)
        .filter(_.forall(java.lang.Character.isLetter)))
  }

  def createVocabulary(words: Dataset[Array[String]], stopWords: Set[String]) = {
    import ss.implicits._
    words
      .flatMap(_.map(_ -> 1L)).rdd
      .reduceByKey(_ + _)
      .filter(word => !stopWords.contains(word._1))
      .map(_._1).collect()
  }

  def createDocuments(tokenized: Dataset[Array[String]], vocab: Map[String, Int]): RDD[(Long, linalg.Vector)] =
    tokenized.rdd.zipWithIndex.map {
      case (tokens, id) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach {
          term =>
            if (vocab.contains(term)) {
              val idx = vocab(term)
              counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
            }
        }
        (id, Vectors.sparse(vocab.size, counts.toSeq))
    }

  def visualizeResult(ldaModel: LDAModel, vocabArray: Array[String]) = {
    ldaModel.describeTopics(maxTermsPerTopic = 10).foreach {
      case (terms, termWeights) =>
        terms.zip(termWeights).foreach {
          case (term, weight) =>
            println(f"${
              vocabArray(term.toInt)
            } [$weight%1.2f]\t")
        }
        println()
    }
  }


  def main(args: Array[String]): Unit = {

    val schema = createSchema

    val rawDataset = getInitialRDD(schema)

    val stopWords: Set[String] = loadStopWords("src/main/resources/stopwords.txt")

    val words = getWords(rawDataset)

    val vocabArray: Array[String] = createVocabulary(words, stopWords)

    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    val tokenized = words
    //TODO: create more advanced tokenized set of words

    // Convert documents into term count vectors
    val documents: RDD[(Long, linalg.Vector)] = createDocuments(tokenized, vocab)

    val lda = new LDA().setK(3).setMaxIterations(100)
    val ldaModel = lda.run(documents)

    visualizeResult(ldaModel,vocabArray)

    ldaModel.save(ss.sparkContext, "src/main/resources/model")
  }

}
