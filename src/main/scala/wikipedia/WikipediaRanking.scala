package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

object WikipediaRanking extends App {

  val languages = List("JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy", "Elixir", "Swift", "Elm")

  val sparkConfiguration: SparkConf = new SparkConf().setAppName("Wikipedia").setMaster("local")
  val sparkContext: SparkContext = new SparkContext(sparkConfiguration)

  val wikiRdd: RDD[WikipediaArticle] = sparkContext.textFile(WikipediaData.filePath).map(WikipediaData.parse)

  def countOccurrencesOfLanguageInArticles(language: String, articlesRdd: RDD[WikipediaArticle]): Int = {
    def reducer(accumulator: Int, article: WikipediaArticle) = if (article.containsWord(language)) accumulator + 1 else accumulator
    def combiner(firstChunkResult: Int, secondChunkResult: Int) = firstChunkResult + secondChunkResult

    articlesRdd.aggregate(0)(reducer, combiner)
  }

  /**
    * Gets the ranking of the languages most mentioned in Wikipedia articles.
    * @param languages List of languages to rank.
    * @param articlesRdd Group of articles used in the ranking.
    * @return List of pairs where the first element is the language and the second the number of articles that mention it at least once.
    */
  def rankLanguages(languages: List[String], articlesRdd: RDD[WikipediaArticle]): List[(String, Int)] =
    languages.map { language =>
      (language, countOccurrencesOfLanguageInArticles(language, articlesRdd))
    }.sortWith((firstTuple, secondTuple) => firstTuple._2 >= secondTuple._2)

  /**
    * Creates an index of languages and the articles that mention it at least once.
    * @param languages List of languages to consider in the index.
    * @param articlesRdd Group of articles used to build the index.
    */
  def makeIndex(languages: List[String], articlesRdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] =
    articlesRdd.flatMap(article => languages.collect {
      case language if article.containsWord(language) => (language, article)
    }).groupByKey()


  def rankLanguagesUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] =
    index
      .mapValues(_.size)
      .sortBy({ case (_, numberOfArticles) => numberOfArticles }, ascending = false)
      .collect()
      .toList


  def rankLanguagesReduceByKey(languages: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =
    rdd.flatMap { article =>
      languages.collect {
        case language if article.containsWord(language) => (language, 1)
      }
    }.reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()
      .toList

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code  // Evaluate
    val stop = System.currentTimeMillis()
    val elapsedTime = stop - start
    timing.append(s"Processing $label took $elapsedTime ms.\n")
    result
  }


  val languagesNaivelyRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLanguages(languages, wikiRdd))

  /* An inverted index mapping languages to wikipedia pages on which they appear */
  def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(languages, wikiRdd)

  val languagesRankedWithInvertedIndex: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLanguagesUsingIndex(index))

  val languagesRankedWithReduceByKey: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLanguagesReduceByKey(languages, wikiRdd))

  /* Output the speed of each ranking */
  println("--------------- RESULTS -----------------")

  println(s"Languages naively ranked: ${languagesNaivelyRanked.mkString(", ")}")
  println(s"Languages ranked using an inverted index: ${languagesRankedWithInvertedIndex.mkString(", ")}")
  println(s"Languages ranked using reduceByKey: ${languagesRankedWithReduceByKey.mkString(", ")}")

  println(timing)
  sparkContext.stop()
}
