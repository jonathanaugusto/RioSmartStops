package RioSmartStops

/**
 * UFRJ - Escola Politécnica
 * "Big Data" course
 * Professor: Villas Boas, Sergio B. [sbVB]
 * Student: Jonathan Augusto <jonathanaugusto@poli.ufrj.br>
 * Project: RioSmartStops
 * Date: 25/06/15
 */

class BusLine (lineId: String, dir: Array[String]) {

  val id: String = lineId
  val direction: Array[String] = dir

}

class Bus (busId: String, line: BusLine, vel: Float){

  val id: String = busId
  var busLine: BusLine = line
  var velocity: Float = vel
}

class GPSPoint (b: Bus, lt: Float, ln: Float, dtTm: String){
  val bus: Bus = b
  val lat: Float = lt
  val long: Float = ln
  val dateTime: String = dtTm
}

class GPSData (dttm: String, id: String, line: String, lt: Float, ln: Float, v: Float) {
  // ["06-25-2015 00:38:02","D87530","",-22.91769,-43.60775,0.0]
  val dateTime: String = dttm
  val busId: String = id
  val busLine: String = line
  val lat: Float = lt
  val lon: Float = ln
  val vel: Float = v

//  def this(list: List[Nothing]) {
//
//    this(dttm: list[0], id: list[1], line: list[2], lt: list[3], ln: list[4], v: list[5])
//  }

}
