/**
 * @author jonny
 *
 *         UFRJ - Escola Politécnica
 *         "Big Data" course
 *         Professor: Villas Boas, Sergio B. [sbVB]
 *         Student: Jonathan Augusto <jonathanaugusto@poli.ufrj.br>
 *         Project: RioSmartStops
 *         Date: 23/06/15
 */

package RioSmartStops


/*
object Main {
  def main(args: Array[String]) {
    val logFile = "README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Example1").setMaster("yarn-client")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
} */

// Main class for RioSmartStops
object Main {

  def main(args: Array[String]) {

    Functions.GetGPSData()
    Functions.FormatGPSData()

    //Functions.FindNextBuses(-22.876705,-43.335793,200) // Viaduto de Madureira
    Functions.FindNextBuses(-23.001494, -43.366088,200) // Alvorada
    //Functions.FindNextBuses(-22.901285, -43.179065,200) // Candelaria
    //Functions.FindNextBuses(-22.860928, -43.227278,200) // Bloco H



    Global.stop()
  }

}
