package timeusage

import org.apache.spark.sql.DataFrame
import org.junit.Test
import timeusage.TimeUsage._

class TimeUsageSuite {

  lazy val dataFrame: (List[String], DataFrame) = TimeUsage.read("src/test/resources/timeusage/atussum_test.csv")

  @Test def `classifiedColumns test`(): Unit = {
    val (primary, working, leisure) = classifiedColumns(dataFrame._1)

    assert(primary.size == 55)
    assert(working.size == 23)
    assert(leisure.size == 346)
  }

  @Test def `compare timeUsage`(): Unit = {
    val (primary, working, leisure) = classifiedColumns(dataFrame._1)
    val summaryDf = timeUsageSummary(primary, working, leisure, dataFrame._2).persist()

    val dfGrouped = timeUsageGrouped(summaryDf).persist()

    val dfGroupedSql = timeUsageGroupedSql(summaryDf)

    val summaryDs = timeUsageSummaryTyped(summaryDf)
    val dsGrouped = timeUsageGroupedTyped(summaryDs)

    assert(dfGrouped.show() == dfGroupedSql.show())
    assert(dfGrouped.show() == dsGrouped.show())
  }

  //  +-----------+------+------+------------+----+-----+
  //  |    working|   sex|   age|primaryNeeds|work|other|
  //  +-----------+------+------+------------+----+-----+
  //  |not working|female|active|        12.3| 0.5| 11.2|
  //  |not working|female| elder|        10.9| 0.3| 12.3|
  //  |not working|female| young|        11.7| 0.3| 11.9|
  //  |not working|  male|active|        11.2| 1.1| 11.8|
  //  |not working|  male| elder|        10.6| 0.8| 12.1|
  //  |not working|  male| young|        11.6| 0.1| 12.2|
  //  |    working|female|active|        11.5| 4.2|  8.7|
  //  |    working|female| elder|        10.7| 4.0|  9.4|
  //  |    working|female| young|        11.4| 3.2|  9.6|
  //  |    working|  male|active|        10.7| 5.3|  8.4|
  //  |    working|  male| elder|        10.3| 5.2|  8.7|
  //  |    working|  male| young|        10.8| 3.8|  9.6|
  //  +-----------+------+------+------------+----+-----+
}
