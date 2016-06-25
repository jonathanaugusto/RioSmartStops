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

import java.sql.Timestamp

import RioSmartStops.Global._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import scala.collection.mutable._


// GPS Data Updater Program
object GPSDataUpdater {

  def main(args: Array[String]) {

    PushGPSData()


    //FindNearBuses(-22.876705,-43.335793,200) // Viaduto de Madureira
    //FindNearBuses(-23.001494, -43.366088,200) // Alvorada
    //FindNearBuses(-22.901285, -43.179065,200) // Candelaria
    //FindNearBuses(-22.860928, -43.227278,200) // Bloco H

    stop()
  }


  def PushGPSData() {

    if (debug) println("Refreshing GPS data...")

    //TODO: Pensar em como coletar os dados de GPS rapidamente: um cluster, data warehouse, Hive, Kafka, Plume, algum desses citados pelo Spark. 50 SEGUNDOS pra baixar cada arquivo!

    val gpsdata_json: DataFrame = SparkSqlContext.read.json(Test_GPS_Data_File)

    // Select "DATA" row and map data
    //"06-27-2015 00:01:10","A63535","",-22.867781,-43.258301,0.0
    val gpsdata_all_arr = gpsdata_json.select("DATA").first()
      .getAs[WrappedArray[(String, String, String, Double, Double, Double, String)]](0)

    val last_execution_str = SparkSqlContext.read.
      jdbc(DBConnectionString, "gpsdata", DBConnectionProperties)
      .selectExpr("max(datetime)").collect()(0).getAs[String](0)

    var last_execution_timestamp: Timestamp = null

    var gpsdata_rdd: RDD[GPSData] = null

    if (last_execution_str != null) {
      last_execution_timestamp = formatTimestamp(last_execution_str)
      println(last_execution_str)
      gpsdata_rdd = SparkGlobalContext.parallelize(gpsdata_all_arr)
        .map(f => GPSData(formatTimestamp(f._1), f._2, f._3, f._4, f._5, f._6))
        .filter(f => f.datetime.compareTo(last_execution_timestamp) > 0)
    }
    else
      gpsdata_rdd = SparkGlobalContext.parallelize(gpsdata_all_arr)
        .map(f => GPSData(formatTimestamp(f._1), f._2, f._3, f._4, f._5, f._6))

    //            GPSDataQueue.synchronized(GPSDataQueue += gpsdata_rdd)

    //            var gpsdata_df: DataFrame = SparkSqlContext.createDataFrame(gpsdata_rdd)
    //            gpsdata_df.printSchema()

    //            gpsdata_df.write.mode("overwrite").jdbc(DBConnectionString, "gpsdata", DBConnectionProperties)

    //            gpsdata_json.unpersist()
    //            gpsdata_rdd.unpersist()
    //            gpsdata_df.unpersist()


  }

  def PullGPSData() {
    //    val GPSDataStream: InputDStream[GPSData] = SparkStreamingContext.queueStream(GPSDataQueue, oneAtATime = true)

    // TODO: 1) Calcular distâncias de Stop em relação a Shape (guardar no banco) => outra função
    // TODO: 2) Como manter tabela Shape em memória?
    // TODO: 3) Comparar distâncias GPSData e Shape(dist de ponto a reta e depois projeção)

  }



}

