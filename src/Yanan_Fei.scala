import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Yanan_Fei {

  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()

    /////////////////// create spark context ///////////////////
    val sparkConf = new SparkConf()
      .setAppName("Yanan_Fei")
      .setMaster("local")
      .set("spark.driver.host", "localhost")
    val sc = new SparkContext(sparkConf)

    ////////////////// read data /////////////////////
    val DataFile = sc.textFile("input/h1b_input.csv")
    val Header = DataFile.first()

    // Dumb way to find index
    val headerList = Header.toUpperCase.split(";")
    var SOCNameIndex = -1
    var caseStatusIndex = -1
    var workStateIndex = -1
    for (i <- 0 until(headerList.length - 1)) {
      if (SOCNameIndex == -1 && headerList(i).contains("SOC_NAME")) {
        SOCNameIndex = i
      }

      else if (caseStatusIndex == -1 && headerList(i).contains("STATUS")) {
        caseStatusIndex = i
      }

      else if (workStateIndex == -1 && headerList(i).contains("WORK") && headerList(i).contains("STATE")) {
        workStateIndex = i
      }

    }
    // println(SOCNameIndex)
    // println(caseStatusIndex)
    // println(workState)

    val Data = DataFile.filter(x => x != Header)
      .map(x => x.split(";"))

    //////////////////////////// Occupations ///////////////////////////////////
    val occupationsData = Data
      .filter(x => x(caseStatusIndex).toUpperCase == "CERTIFIED" && x(SOCNameIndex) != "")
      .map(x => (x(SOCNameIndex).replaceAll("\"", "").toUpperCase, 1))
      .reduceByKey(_ + _)
    // Data.take(10).foreach(println)

    val occupationsTotal = occupationsData.map(x => x._2).sum()
    // println(max)
    // 451442.0

    val wholeOccupationssResults = occupationsData.map{
      case (socName, numCertified) =>
        (socName, numCertified, (numCertified / occupationsTotal) * 100)
    }.sortBy(- _._2)
    // wholeResults.take(10).foreach(println)

    var numTopOccupations = 10
    val lenOccupations = wholeOccupationssResults.collect.length
    if (lenOccupations < numTopOccupations) {
      numTopOccupations = lenOccupations
    }
    val topTenOccupations = wholeOccupationssResults.take(numTopOccupations)
      .map(x => (x._2, x._1, x._3))
      .sortWith((x, y) =>
        (x._1 > y._1) ||
          (x._1 == y._1 && x._2 < y._2))
    // topTenOccupations.foreach(println)

    //////////////////////////// States //////////////////////////
    val statesData = Data
      .filter(x => x(caseStatusIndex).toUpperCase == "CERTIFIED" && x(workStateIndex) != "")
      .map(x => (x(workStateIndex).replaceAll("\"", "").toUpperCase, 1))
      .reduceByKey(_ + _)
    // StatesData.take(10).foreach(println)

    val statesTotal = statesData.map(x => x._2).sum()

    val wholeStatesResults = statesData.map{
      case (stateName, numCertified) =>
        (stateName, numCertified, (numCertified / statesTotal) * 100)
    }.sortBy(- _._2)

    var numTopStates = 10
    val lenStates = wholeStatesResults.collect.length
    if (lenStates < numTopStates) {
      numTopStates = lenStates
    }
    val topTenStates = wholeStatesResults.take(10)
      .map(x => (x._2, x._1, x._3))
      .sortWith((x, y) =>
        (x._1 > y._1) ||
          (x._1 == y._1 && x._2 < y._2))

    /////////////////// save ////////////////////////
    val outPut_topTenOccupations = "TOP_OCCUPATIONS;NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE\n" +
      topTenOccupations.map(x => {
        x._2 + ";" + x._1 + ";" + "%.1f%%".format(x._3) + "\n"
    }).reduce(_ + _)

    new PrintWriter("output/top_10_occupations.txt") {
      write(outPut_topTenOccupations)
      close()
    }

    val outPut_topTenStates = "TOP_STATES;NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE\n" +
      topTenStates.map(x => {
        x._2 + ";" + x._1 + ";" + "%.1f%%".format(x._3) + "\n"
      }).reduce(_ + _)

    new PrintWriter("output/top_10_states.txt") {
      write(outPut_topTenStates)
      close()
    }

    /////////////////// running time ////////////////
    val time = (System.nanoTime() - startTime) / 1e9d
    println(s"Time is $time seconds \n")

  }

}
