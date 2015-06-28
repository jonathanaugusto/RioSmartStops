package RioSmartStops

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

/**
 * UFRJ - Escola Politécnica
 * "Big Data" course
 * Professor: Villas Boas, Sergio B. [sbVB]
 * Student: Jonathan Augusto <jonathanaugusto@poli.ufrj.br>
 * Project: RioSmartStops
 * Date: 25/06/15
 */

// General-purposes classes for RioSmartStops.
// I think it can be removed, it's not needed... :/


class BusLine (lineId: String, dir: Array[String]) {

  val id: String = lineId
  val direction: Array[String] = dir

}

class Bus (busId: String, line: BusLine, vel: Float){

  val id: String = busId
  var busLine: BusLine = line
  var velocity: Float = vel
}

class GPSPoint (lt: Float, ln: Float){
  val lat: Float = lt
  val lon: Float = ln
}

class GPSData(dtm: String, id: String, line: String, lt: Float, ln: Float, vl: Float) {
  // ["06-25-2015 00:38:02","D87530","",-22.91769,-43.60775,0.0]

  var dateTime: Timestamp = format(dtm)
  var busId: String = id
  var busLine: String = line
  var lat: Float = lt
  var lon: Float = ln
  var vel: Float = vl

  val toFloat = udf[Float, String]( _.toFloat)
  //val toDate = udf[Date, String](format.parse(_))
  val udfRemoveZero = udf((id:String) => id.substring(0,id.indexOf(".")))

  def removeZero (id:String) : String = {
    if (id.contains("."))
    return id.substring(0,id.indexOf("."))
    id
  }

  def format (s: String) : Timestamp ={
    //"06-25-2015 00:38:02" to "2015-06-25 00:38:02"
    val formatted = s.substring(6,10)+"-"+s.substring(0,5)+s.substring(10)
    Timestamp.valueOf(formatted)
  }


  def this (l: List[String]) {
    this (l.head.toString,l(1).toString,l(2).toString,
      l(3).toString.toFloat,l(4).toString.toFloat,l(5).toString.toFloat)
  }

  def this (r: Row) {
    this (r(0).toString,r(1).toString,r(2).toString,
      r(3).toString.toFloat,r(4).toString.toFloat,r(5).toString.toFloat)
  }

  def this() {
    this ("01-01-1970 00:00:00", "", "", 0, 0, -1)
  }


  override def toString: String = "DATETIME:%s | ID:%s | LINE:%s | LAT:%f | LON:%f | VEL:%f".format(dateTime, busId, busLine, lat, lon, vel)


}
