package stackoverflow

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._
import stackoverflow.StackOverflowSuite.sc

object StackOverflowSuite {
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow-Test")
  val sc: SparkContext = new SparkContext(conf)
}

class StackOverflowSuite {

  lazy val testObject: StackOverflow = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  lazy val lines: RDD[String] = sc.textFile("src/test/resources/stackoverflow/stackoverflow_test.csv")
  lazy val rawPostings: RDD[Posting] = testObject.rawPostings(lines)
  lazy val groupedPostings: RDD[(QID, Iterable[(Question, Answer)])] = testObject.groupedPostings(rawPostings)
  lazy val scoredPostings: RDD[(Question, HighScore)] = testObject.scoredPostings(groupedPostings)
  lazy val vectorPostings: RDD[(LangIndex, HighScore)] = testObject.vectorPostings(scoredPostings)

  @Test def `testObject can be instantiated`(): Unit = {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }



  @Test def `testGroupedPostings`(): Unit = {
    println(groupedPostings.toDebugString)

    assert(groupedPostings.count() == 26075)
  }

  @Test def `testScoredPostings`(): Unit = {
    println(scoredPostings.toDebugString)

    assert(scoredPostings.count() == 26075)
  }

  @Test def `testVectorPostings`(): Unit = {
    println(vectorPostings.toDebugString)

    assert(vectorPostings.count() == 26075)
  }

  @Rule def individualTestTimeout = new org.junit.rules.Timeout(100 * 1000)
}
