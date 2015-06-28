/**
 * UFRJ - Escola Politécnica
 * "Big Data" course
 * Professor: Villas Boas, Sergio B. [sbVB]
 * Student: Jonathan Augusto <jonathanaugusto@poli.ufrj.br>
 * Project: RioSmartStops
 * Date: 24/06/15
 */

package RioSmartStops

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import scala.math._

// Global values, variables and IO/formatting functions
// Application-specific functions (i.e. manipulating
// GPS data) can be found at Functions.scala
object Global {

  // URI to file with GPS raw data
  val GPSRawDataHttpFile = "http://dadosabertos.rio.rj.gov.br/apiTransporte/apresentacao/rest/index.cfm/obterTodasPosicoes"

  // Local directory to save all files
  val GPSLocalDir = "hdfs://JonnyLaptop:9000/user/jonny/data/RioSmartStops/"

  // File name for GPS raw data
  val GPSRawDataLocalFile = GPSLocalDir + "GPSRawData.json"

  // File name for GPS formatted data
  val GPSDataFrameFile = GPSLocalDir + "GPSData.parquet"
  val GPSDataFrameJSONFile = GPSLocalDir + "GPSData.json"

  // Spark configuration
  val sparkConf = new SparkConf().setAppName("RioSmartStops").setMaster("spark://JonnyLaptop:7077")

  // Global contexts
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(Global.sparkContext)

  // DataFrame for GPS formatted data
  var GPSDataFrame: DataFrame = null

  // Boolean for debugging purposes
  val debug = false

  // Boolean in order to set download mode for GPS raw data
  val getNewData = false

  // Format bus line codes (remove ".0" suffix)
  def removeZero (id:String) : String = {
    if (id.contains("."))
      return id.substring(0,id.indexOf("."))
    id
  }

  // Format timestamp string (MM-dd-yyyy-... to yyyy-MM-dd-...)
  // and return Timestamp object
  def format (s: String) : Timestamp ={
    //"06-25-2015 00:38:02" to "2015-06-25 00:38:02"
    val formatted = s.substring(6,10)+"-"+s.substring(0,5)+s.substring(10)
    Timestamp.valueOf(formatted)
  }

  // Dataframe struct
  val GPSDataFrameStruct =
    StructType(
      StructField("datetime", TimestampType, false) ::
        StructField("id", StringType, false) ::
        StructField("line", StringType, true) ::
        StructField("lat", DoubleType, true) ::
        StructField("lon", DoubleType, true) ::
        StructField("vel", DoubleType, true) :: Nil)

  // Dataframe struct
  val GPSDistanceStruct =
    StructType(
      StructField("datetime", TimestampType, false) ::
        StructField("id", StringType, false) ::
        StructField("line", StringType, true) ::
        StructField("dist", DoubleType, true) ::
        StructField("vel", DoubleType, true) :: Nil)

  // GPS raw data struct (all string)
  val GPSDataFrameJSONFileStruct =
    StructType(
      StructField("datetime", StringType, false) ::
        StructField("id", StringType, false) ::
        StructField("line", StringType, true) ::
        StructField("lat", StringType, true) ::
        StructField("lon", StringType, true) ::
        StructField("vel", StringType, true) :: Nil)


  // Haversine distance (in meters)
  def Distance (lat1:Double, lon1:Double, lat2:Double, lon2:Double): Double={
    val R = 6372800.0  // Earth radius
    val dLat=(lat2 - lat1).toRadians
    val dLon=(lon2 - lon1).toRadians

    val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2 * asin(sqrt(a))
    R * c
  }


  def saveDataFrameAsFile () {
    Global.GPSDataFrame.select("*").write.mode(SaveMode.Overwrite).save(Global.GPSDataFrameFile)
    Global.GPSDataFrame.select("*").write.format("json").mode(SaveMode.Overwrite).save(Global.GPSDataFrameJSONFile)
  }

  // Stop Spark context
  def stop() {
    Global.sparkContext.stop()
  }
}
