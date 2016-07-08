/**
  * @author jonny
  *
  *         UFRJ - Escola Politécnica
  *         "Big Data" course
  *         Professor: Villas Boas, Sergio B. [sbVB]
  *         Student: Jonathan Augusto <jonathanaugusto@poli.ufrj.br>
  *         Project: RioSmartStops
  */

package RioSmartStops

import java.net.URL

import RioSmartStops.Global._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkContext}

import scala.collection.mutable._
import scala.io.Source

// GPS Data Updater Program
object GPSDataUpdater {

  val sc = SparkContext.getOrCreate(SparkConf.setAppName("RioSmartStops-GPS"))
  val sqlc = SQLContext.getOrCreate(sc)
  val ssc = new StreamingContext(sc, Seconds(30))

//  def createSSC(): StreamingContext = {
//    val ssc = new StreamingContext(sc, Seconds(30))
//    ssc.checkpoint(Local_Dir + Checkpoint_Dir)
//    ssc
//  }
//  val ssc = StreamingContext.getOrCreate(Local_Dir + Checkpoint_Dir, createSSC _)

  def main(args: Array[String]) {

    if (debug) print("Updating GPS data... ")
    // Select "DATA" row and map data
    //    //"06-27-2015 00:01:10","A63535","",-22.867781,-43.258301,0.0,""

    val inputStream = ssc.receiverStream(new GPSDataPusher())
//    inputStream.foreachRDD(f => f.foreach(println))
    inputStream.print()
    ssc.start()
    ssc.awaitTermination()
//    ssc.stop(stopSparkContext = true, stopGracefully = true)

    //FindNearBuses(-22.876705,-43.335793,200) // Viaduto de Madureira
    //FindNearBuses(-23.001494, -43.366088,200) // Alvorada
    //FindNearBuses(-22.901285, -43.179065,200) // Candelaria
    //FindNearBuses(-22.860928, -43.227278,200) // Bloco H

  }

  class GPSDataPusher() extends Receiver[DataFrame](StorageLevel.MEMORY_AND_DISK) with Logging {

    def onStart() {
      new Thread("GPS Data Pusher Thread") {
        override def run() { receive() }
      }.start()
    }

    def onStop() {
      // There is nothing much to do as the thread calling receive()
      // is designed to stop by itself if isStopped() returns false
    }

    private def receive() {

      try {
        while(!isStopped) {
          //TODO: Pensar em como coletar os dados de GPS rapidamente: um cluster, data warehouse, Hive, Kafka, Plume, algum desses citados pelo Spark. 50 SEGUNDOS pra baixar cada arquivo!

          // Select "DATA" row and map data
          //    //"06-27-2015 00:01:10","A63535","",-22.867781,-43.258301,0.0,""

          //          var gpsdata_text = sc.textFile(GPS_Bus_Data_File)
//          val charset = new Charset("decoder",{("%22", '\"'), ("%20", " "), ("%5B", "["), ("%5D", "]") })
//          val codec = new Codec(charset)
          val url = new URL(GPS_Test_Data_File)
          val html = Source.fromURL(url)("CP1252").bufferedReader().readLine()
//            .replaceAll("%22", "\"").replaceAll("%20", " ").replaceAll("%5B", "[").replaceAll("%5D", "]")
          //          val gpsdata_text = html.getLines()
          var gpsdata_json = sqlc.read.json(html)
          var gpsdata_rdd = gpsdata_json.select("DATA")
            .map(f => f(0).asInstanceOf[WrappedArray[WrappedArray[String]]])
            .map(f => f(0))
            .map(f => GPSData(formatTimestamp(f(0)), f(1), formatBusLine(f(2)), f(3).toDouble, f(4).toDouble, f(5).toDouble, f(6)))

//          if (isStarted()) gpsdata_rdd.foreach(f => store(f))

          //      gpsdata_df.show()
          //      gpsdata_df.write.mode("overwrite").parquet(Parquet_GPSData_File)
          //      gpsdata_df.write.mode(SaveMode.Append).jdbc(DBConnectionString,"gpsdata_",DBConnectionProperties)

          val gpsdata_df = sqlc.createDataFrame(gpsdata_rdd).persist(this.storageLevel)
//          gpsdata_df.show()
          store(gpsdata_df)
          logInfo("Received " + gpsdata_rdd.count() + " records")

          gpsdata_rdd.unpersist()
          gpsdata_json.unpersist()
//          gpsdata_text.unpersist()

          gpsdata_rdd = null
          gpsdata_json = null
//          gpsdata_text = null


          logInfo("Stopped receiving")
          Thread.sleep(30000)

        }
      } catch {
        case t: Throwable =>
          // restart if there is any other error
          logError("Error pushing data", t)
          restart("Error pushing data", t)
      }

    }
  }

  def PullGPSData() {

    // TODO: 1) Calcular distâncias de Stop em relação a Shape (guardar no banco) => outra função
    // TODO: 2) Como manter tabela Shape em memória?
    // TODO: 3) Comparar distâncias GPSData e Shape(dist de ponto a reta e depois projeção)

    if (debug) print("Updating GPS data... ")
    val inputStream = ssc.receiverStream(new GPSDataPusher())
    inputStream.print()
//    val df = sqlc.createDataFrame(inputStream.asInstanceOf[RDD[GPSData]])
//    df.show()
    ssc.start()
//    ssc.awaitTerminationOrTimeout(2000)
  }



}

