/**
 * UFRJ - Escola Politécnica
 * "Big Data" course
 * Professor: Villas Boas, Sergio B. [sbVB]
 * Student: Jonathan Augusto <jonathanaugusto@poli.ufrj.br>
 * Project: RioSmartStops
 * Date: 24/06/15
 */

package RioSmartStops

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._

import scala.io.{BufferedSource, Source}
import scala.math._

// Functions to operate and manipulate GPS data.
// Global variables and minor IO/formatting functions
// can be found at Global.scala

object Functions {

  // Download raw data if boolean setted true and
  // remote and local files are different.

  def GetGPSData() {

    if (Global.debug) println("BEGIN GetGPSData")

    // Get http file and save to hdfs
    val conf = new Configuration()
    conf.set("fs.defaultFS", Global.GPSLocalDir)

    val fs = FileSystem.get(conf)
    val path = new Path(Global.GPSRawDataLocalFile)

    if (Global.getNewData) {

      // File not exists => Create new file
      // File exists
      //  |- Hash is different => Create new file
      //  |- Hash is the same => Nothing to do

      // Trying to connect in order to get new GPS raw data
      var source: BufferedSource = null
      try source = Source.fromURL(Global.GPSRawDataHttpFile)
      catch {
        case e: Exception => println("Unable to connect")
      }
      finally

      // If connection was successful, verify hashcodes of local and remote files.
      // If both are different, download new data
        if ((source != null) && !(fs.exists(path) && fs.hashCode() == source.hashCode())) {
          println("Retrieving new data...")
          val os = fs.create(path)
          source.foreach(c => os.write(c))
          os.write('\n')
          os.flush()
          os.close()
        }
    }
    if (Global.debug) println("END GetGPSData")

  }

  // Format raw data with defined structure:
  // DATAHORA: string   => datetime: Java timestamp
  // --- ["06-25-2015 00:38:02" to "2015-06-25 00:38:02"]
  // ORDEM: string      => id: string
  // LINHA: string      => line: string
  // LATITUDE: string   => lat: double
  // LONGITUDE: string  => lon: double
  // VELOCIDADE: string => vel: double

  def FormatGPSData() {

    if (Global.debug) println("BEGIN FormatGPSData\n")

    // Read JSON raw data
    val jsonFile = Global.sqlContext.read.json(Global.GPSRawDataLocalFile)

    // Select "DATA" row and map data
    val list = jsonFile.select("DATA")
      .collectAsList().get(0).getAs[Seq[List[String]]](0)
      .map(f => Row(Global.format(f.head), f(1), Global.removeZero(f(2)), f(3).toDouble, f(4).toDouble, f(5).toDouble))

    // Create RDD and DataFrame
    val rdd = Global.sparkContext.parallelize(list)
    Global.GPSDataFrame = Global.sqlContext.createDataFrame(rdd, Global.GPSDataFrameStruct)
    Global.GPSDataFrame.registerTempTable("GPSData")

    //    val a = Global.sqlContext.sql("SELECT * FROM GPSData WHERE line = ''")
    //    val count: Double = a.count() * 100 / df.count()
    //    println("Carros sem linha informada: " + a.count() + "(" + count + "%)")

    if (Global.debug) println("END FormatGPSData")
  }

  //
  def FindNextBuses(lat: Double, lon: Double, radius: Double): Unit = {
    if (Global.debug) println("BEGIN FindNextBuses")

    //    val df = Global.GPSDataFrame
    //    val rdd = df.map(f => Row(f(0),f(1),f(2),
    //      Global.Distance(f(3).toString.toDouble,f(4).toString.toDouble,lat,lon),
    //      f(5)))
    //    val df2 = Global.sqlContext.createDataFrame(rdd, Global.GPSDistanceStruct).registerTempTable("DistanceData")
    //    val a = Global.sqlContext.sql("SELECT datetime,id,line,dist FROM DistanceData ORDER BY dist ASC")
    //    a.show()


    Global.sqlContext.udf.register("distance", (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
      // Haversine distance (in meters)
      val R = 6372800.0 // Earth radius
      val dLat = (lat2 - lat1).toRadians
      val dLon = (lon2 - lon1).toRadians
      val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
      val c = 2 * asin(sqrt(a))
      R * c
    })

    val df2 = Global.sqlContext.sql("SELECT datetime,id,line FROM GPSData WHERE distance(lat,lon,"
      + lat.toString + "," + lon.toString + ") <= " + radius.toString)
    df2.registerTempTable("NextBuses")

    if (Global.debug) println("END FindNextBuses")
  }

}