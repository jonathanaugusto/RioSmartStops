package RioSmartStops

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
 * UFRJ - Escola Politécnica
 * "Big Data" course
 * Professor: Villas Boas, Sergio B. [sbVB]
 * Student: Jonathan Augusto <jonathanaugusto@poli.ufrj.br>
 * Project: RioSmartStops
 * Date: 24/06/15
 */
object Global {

  val GPSHttpFile = "http://dadosabertos.rio.rj.gov.br/apiTransporte/apresentacao/rest/index.cfm/obterTodasPosicoes"
  val GPSLocalDir = "hdfs://localhost:9000/user/jonny/data/"
  val GPSLocalFile = GPSLocalDir + "GPSdata.json"


  def getGPSData(): Unit = {
    val sparkConf = new SparkConf().setAppName("RioSmartStops").setMaster("spark://JonnyLaptop:7077")
    val sparkContext = new SparkContext(sparkConf)

    // Get http file and save to hdfs

    val conf = new Configuration()
    conf.set("fs.defaultFS", GPSLocalDir)

    val fs = FileSystem.get(conf)
    val path = new Path(GPSLocalFile)
    val source = Source.fromURL(GPSHttpFile)

    // File not exists => Create new file
    // File exists
    //  |- Hash is different => Create new file
    //   - Hash is the same => Nothing to do

    if (!(fs.exists(path) == true && fs.hashCode() == source.hashCode())) {
      println("Retrieving new data...")
      val os = fs.create(path)
      source.foreach(c => os.write(c))
      os.flush()
      os.close()
    }
    sparkContext.stop()
  }

  def mapGPSData (): Unit ={
    // Format hdfs file
    val sparkConf = new SparkConf().setAppName("RioSmartStops").setMaster("yarn-client")
    val sparkContext = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    val jsonFile = sqlContext.read.json(GPSLocalFile)
    val list = jsonFile.select("COLUMNS").collect()
    list.foreach{println}

    sparkContext.stop()
  }

  def stop(): Unit ={
    //sparkContext.stop()
  }

}
