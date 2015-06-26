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
  val GPSLocalDir = "hdfs://JonnyLaptop:9000/user/jonny/data/"
  val GPSLocalFile = GPSLocalDir + "GPSdata.json"
  val sparkConf = new SparkConf().setAppName("RioSmartStops-GetGPSData").setMaster("spark://JonnyLaptop:7077")
  val sparkContext = new SparkContext(sparkConf)
  val debug = true
  val getNewData = false

  def getGPSData(): Unit = {
    if (debug) println("BEGIN getGPSData")
    // Get http file and save to hdfs

    val conf = new Configuration()
    conf.set("fs.defaultFS", GPSLocalDir)

    val fs = FileSystem.get(conf)
    val path = new Path(GPSLocalFile)

    // File not exists => Create new file
    // File exists
    //  |- Hash is different => Create new file
    //  |- Hash is the same => Nothing to do

  if (getNewData) {
    val source = Source.fromURL(GPSHttpFile)
    if (!(fs.exists(path) && fs.hashCode() == source.hashCode())) {
      println("Retrieving new data...")
      val os = fs.create(path)
      source.foreach(c => os.write(c))
      os.flush()
      os.close()
    }
  }
    //sparkContext.stop()
    if (debug) println("END getGPSData")

  }

  def mapGPSData (): Unit ={
    // Format hdfs file
    if (debug) println("BEGIN mapGPSData")

   val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    val jsonFile = sqlContext.read.json(GPSLocalFile)
    //val list = jsonFile.select("COLUMNS").collect()
    //list.foreach{println}
    val list = jsonFile.select("DATA").collectAsList().get(0).getList(0) // .get(x)
    val obj = list.get(3)

    println(obj)//describe("DATETIME", "BUSID", "BUSLINE", "LAT", "LONG", "VELOCITy")
    //val list = jsonFile.select("DATA").describe("DATETIME", "BUSID", "BUSLINE", "LAT", "LONG", "VELOCITy")


    //sparkContext.stop()

    if (debug) println("END mapGPSData")
  }

  def stop(): Unit ={
    sparkContext.stop()
  }

}
