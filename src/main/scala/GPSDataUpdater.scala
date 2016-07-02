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

import RioSmartStops.Global._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkContext}

import scala.collection.mutable._

// GPS Data Updater Program
object GPSDataUpdater {

  val sc = SparkContext.getOrCreate(SparkConf.setAppName("RioSmartStops-GPS"))
  val sqlc = SQLContext.getOrCreate(sc)
  val ssc = new StreamingContext(sc, Seconds(30))

//  var GPSDataQueue = new Queue[RDD[GPSData]]()

//  def createSSC(): StreamingContext = {
//    val ssc = new StreamingContext(sc, Seconds(30))
//    ssc.checkpoint(Local_Dir + Checkpoint_Dir)
//    ssc
//  }
//  val ssc = StreamingContext.getOrCreate(Local_Dir + Checkpoint_Dir, createSSC _)

  def main(args: Array[String]) {

    if (debug) print("Updating GPS data... ")
    val inputStream = ssc.receiverStream(new GPSDataPusher())
    inputStream.foreachRDD(f => f.foreach(a => a.show(10)))
//    inputStream.print()
//    sc.parallelize[GPSData](inputStream)
    ssc.start()
    ssc.awaitTermination()
    //    val df = sqlc.createDataFrame(inputStream.asInstanceOf[RDD[GPSData]])
    //    df.show()
    //    ssc.awaitTerminationOrTimeout(2000)

//    PullGPSData()

//    ssc.stop(stopSparkContext = true, stopGracefully = true)

    //FindNearBuses(-22.876705,-43.335793,200) // Viaduto de Madureira
    //FindNearBuses(-23.001494, -43.366088,200) // Alvorada
    //FindNearBuses(-22.901285, -43.179065,200) // Candelaria
    //FindNearBuses(-22.860928, -43.227278,200) // Bloco H

  }

  class GPSDataPusher() extends Receiver[DataFrame](StorageLevel.MEMORY_AND_DISK) with Logging {

    def onStart() {
      new Thread("GPS Data Pusher Thread") {
        override def run() {
          receive()
        }
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
          var gpsdata_json = sqlc.read.json(GPS_Bus_Data_File)

          val gpsdata_rdd = gpsdata_json
            .explode("DATA", "NEWDATA") { l: WrappedArray[WrappedArray[String]] => l }
            .select("NEWDATA")
            .map(f => f(0).asInstanceOf[WrappedArray[String]])
            .map(f => GPSData(formatTimestamp(f(0)), f(1), f(2), f(3).toDouble, f(4).toDouble, f(5).toDouble, f(6)))
//          gpsdata_rdd.foreach(store)

                //    gpsdata_df.show()
          //      gpsdata_df.write.mode("overwrite").parquet(Parquet_GPSData_File)
          //      gpsdata_df.write.mode("overwrite").jdbc(DBConnectionString, "gpsdata", DBConnectionProperties)

          val gpsdata_df = sqlc.createDataFrame(gpsdata_rdd)
          store(gpsdata_df)

          //      GPSDataQueue.synchronized(GPSDataQueue += gpsdata_rdd)
          gpsdata_json.unpersist()
          gpsdata_json = null

          logInfo("Stopped receiving")
          restart("Trying to connect again")

          //            gpsdata_json.unpersist()
          //            gpsdata_rdd.unpersist()
          //            gpsdata_df.unpersist()
        }
      } catch {
        case t: Throwable =>
          // restart if there is any other error
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

