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

//import org.apache.commons.math3.geometry.euclidean.threed.Vector3D
import org.apache.spark.sql.{DataFrame, SQLContext, functions}
import org.apache.spark.{SparkConf, SparkContext}

import scala.math._
import scala.util.Random

// Global values, variables and IO/formatting functions
// Application-specific functions (i.e. manipulating
// GPS data) can be found at Functions.scala
object Global {

  // URIs to GPS and bus line files
  val Bus_GPS_Data_URL = "http://dadosabertos.rio.rj.gov.br/apiTransporte/apresentacao/rest/index.cfm/onibus"
  val BRT_GPS_Data_URL = "http://dadosabertos.rio.rj.gov.br/apiTransporte/apresentacao/rest/index.cfm/brt"
  val GTFS_Package_URL = "http://dadosabertos2.rio.rj.gov.br/dadoaberto/google-transit/google_transit.zip"

  // Local directory to save files
  val Local_Dir = "file:////home/jonny/RioSmartStops/"
  val HDFS_Dir = "hdfs:///usr/local/RioSmartStops/"
  val GTFS_Data = "data/gtfs/"
  val GPS_Data = "data/gps/"
  val Parquet_Data = "data/parquet/"
  val Checkpoint = "bkp/"
  val GTFS_Fare_Attributes_File = Local_Dir + GTFS_Data + "fare_attributes.txt"
  val GTFS_Fare_Rules_File = Local_Dir + GTFS_Data + "fare_rules.txt"
  val GTFS_Routes_File = Local_Dir + GTFS_Data + "routes.txt"
  val GTFS_Shapes_File = Local_Dir + GTFS_Data + "shapes.txt"
  val GTFS_Stops_File = Local_Dir + GTFS_Data + "stops.txt"
  val GTFS_Trips_File = Local_Dir + GTFS_Data + "trips.txt"
  val GTFS_Stop_Times_File = Local_Dir + GTFS_Data + "stop_times.txt"
  val Parquet_Fares_File = HDFS_Dir + Parquet_Data + "fares.parquet"
  val Parquet_Routes_File = HDFS_Dir + Parquet_Data + "routes.parquet"
  val Parquet_Shapes_File = HDFS_Dir + Parquet_Data + "shapes.parquet"
  val Parquet_Stops_File = HDFS_Dir + Parquet_Data + "stops.parquet"
  val Parquet_Trips_File = HDFS_Dir + Parquet_Data + "trips.parquet"
  val Parquet_StopTimes_File = HDFS_Dir + Parquet_Data + "stop_times.parquet"
  val Parquet_TripStops_File = HDFS_Dir + Parquet_Data + "tripstops.parquet"

  // Filenames to local data saved
//  val Bus_GPS_Data_File = Local_Dir + GPS_Data + "onibus.json"
  val BRT_GPS_Data_File = Local_Dir + GPS_Data + "brt.json"
  val Test_GPS_Data_File = Local_Dir + GPS_Data + "test.json"

  // Spark configuration
  val SparkConf = new SparkConf().setAppName("RioSmartStops")
//    .setMaster("yarn-client")
    .setMaster("spark://localhost:7077")
    .set("spark.io.compression.codec", "lz4")
    .set("spark.speculation", "false")
    //.set("spark.driver.memory", "4g")
    //.set("spark.executor.memory", "4g")
    //.set("spark.cleaner.ttl", "6000")

  // Global contexts

  val SparkGlobalContext = SparkContext.getOrCreate(SparkConf)
  val SparkSqlContext = new SQLContext(SparkGlobalContext)
  //  val SparkStreamingContext = new StreamingContext(Checkpoint_Dir, SparkGlobalContext)
  //  var GPSDataQueue: Queue[RDD[GPSData]] = Queue()

  // DataFrame for GPS formatted data
  //  var GPSDataDF: DataFrame = null
  //  var FaresDF: DataFrame = null
  //  var RoutesDF: DataFrame = null
  //  var StopsDF: DataFrame = null
  //  var ShapesDF: DataFrame = null
  //  var TripsDF: DataFrame = null
  //  var TripStopsDF: DataFrame = null
  //var QueuesDataFrame: RDD[DataFrame] = null

  // Boolean for debugging purposes
  val debug = true

  // Boolean in order to set download mode for data
  val getNewData = true
  val getNewGPSData = false

  // DB connection
  val DBConnectionString = "jdbc:mysql://localhost:3306/riosmartstops"
  val DBConnectionProperties = new java.util.Properties
  DBConnectionProperties.setProperty("user", "riosmartstops")
  DBConnectionProperties.setProperty("password", "JoNnY.sbVB")
  DBConnectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")
  DBConnectionProperties.setProperty("useSSL", "false")

  val DBConnection = new com.mysql.jdbc.Driver

  val GlobalRegex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)".r
  // Splitting with commas but not ones inside quotes
  val BRSRegex = "\\bBRS?\\b (([ABCDI0-9])+, ?)+([A-Za-z0-9 -])+".r

  // Format bus line codes (remove "", ".0" suffix and define length >= 3)
  def formatBusLine(id: String): String = {
    var newId = id
    if (id.length <= 2) return ""
    if (id.length <= 4) newId = ("0" * (2 / (id.length - 2))) + newId
    if (id.contains(".")) newId = newId.substring(0, newId.indexOf("."))
    newId
  }

  // Format timestamp string (MM-dd-yyyy-... to yyyy-MM-dd-...)
  // and return Timestamp object
  def formatTimestamp(s: String): Timestamp = {
    //"06-25-2015 00:38:02" to "2015-06-25 00:38:02"
    val formatted = s.substring(6, 10) + "-" + s.substring(0, 5) + s.substring(10)
    Timestamp.valueOf(formatted)
  }

  def busStopId(lat: String, lon: String): String = {
    hash(lat + "," + lon)
  }

  def hash(text: String): String = java.security.MessageDigest.getInstance("MD5")
    .digest(text.getBytes()).map(0xFF & _).map {
    "%02x".format(_)
  }.foldLeft("") {
    _ + _
  }

  def randomDirectionID(): Int = new Random().nextInt(1)

  def remove(p: String) {
    val hadoopConf = SparkGlobalContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(p), true)
    } catch{ case e: Exception =>
      println(e)
    }
  }

  //   sort by order
  object ShapeOrdering extends Ordering[Shape] {
    def compare(a: Shape, b: Shape) = {
      if (a.id.toInt == b.id.toInt)
        a.sequence compare b.sequence
      else
        a.id.toInt compare b.id.toInt
    }
  }

  def showAll(df: DataFrame): Unit = {
    df.show(df.count().toInt)
  }

//  def euclideanDistance(point1: (Double, Double), point2: (Double, Double)): Double = {
    //    // Euclidean distance
    //    val dx = point2._1 - point1._1
    //    val dy = point2._2 - point1._2
    //    math.sqrt(dx * dx + dy * dy) * 100000
    //  }
    //
    //  def projection(point: (Double, Double), point1: (Double, Double), point2: (Double, Double)):
    //  (Double, Double) = {
    //    val delta_lat = point2._1 - point1._1
    //    val delta_lon = point2._2 - point1._2
    //    val d = ((delta_lat * (point._1 - point1._1)) + (delta_lon * (point._2 - point1._2))) / ((delta_lat * delta_lat) + (delta_lon * delta_lon))
    //    val proj_lat = point1._1 + delta_lat * d
    //    val proj_lon = point1._2 + delta_lon * d
    //    //if (proj_lat < point2._1 && proj_lat > point1._1 && proj_lon < point2._2 && proj_lon > point1._2)
    //    (proj_lat, proj_lon)
    //    //(-1, -1)
    //  }

  // Stop Spark context
  def stop() {
    //SparkStreamingContext.stop()
    SparkGlobalContext.stop()
  }

  // ---------------- CLASSES


  /**
    * Class to represent GPS realtime data from Internet
    *
    * @param datetime      Date and time captured position
    * @param serial_number Order number for each bus
    * @param route_code    Short identifier for bus line(see class "Route" below)
    * @param lat           Captured latitude
    * @param lon           Captured longitude
    * @param vel           Captured velocity
    */
  case class GPSData(datetime: Timestamp, serial_number: String,
                     route_code: String, lat: Double, lon: Double, vel: Double)

  /**
    * Class to represent bus lines, aligned with GTFS definition.
    *
    * @param id      Unique string to represent each bus line
    * @param code    Short identifier, like well-known numeric codes
    * @param name    Bus line title with origin and destination
    * @param info    More info about this route (data provided by Fetranspor brings 'Vá de Önibus' URL)
    * @param color   Color for shape drawing
    * @param fare_id ID for fare rule (see "Fare" below)
    */
  case class Route(id: String, code: String, name: String, info: String, color: String, fare_id: String)

  /**
    * Class to represent a bus line trip (origin->destination and destination-> origin are two different trips)
    *
    * @param id        Unique string to represent each trip
    * @param route_id  Identifier to related route (see "Route" above)
    * @param direction Boolean to represent: 0 for O->D; 1 for D->O
    * @param headsign  Destination signing
    * @param shape_id  Identifier to related shape draw (see "Shape" below)
    */
  case class Trip(id: Long, route_id: String, direction: Int, headsign: String, shape_id: String)
  SparkSqlContext.udf.register("onSegment", isOnSegmentFunction)

  /**
    * Class to represent bus stops.
    *
    * @param id   Unique identifier for each stop
    * @param code Abbreviation code
    * @param name Stop name (if any; if not, copied from "Stop description" below)
    * @param desc Stop description (like approximated address)
    * @param lat  Latitude
    * @param lon  Longitude
    */
  case class Stop(id: String, code: String, name: String, desc: String,
                  lat: Double, lon: Double)

  /**
    *
    * @param id
    * @param sequence
    * @param lat1
    * @param lon1
    * @param lat2
    * @param lon2
    * @param dist
    */
  case class Shape(id: String, sequence: Long, lat1: Double, lon1: Double,
                   lat2: Double, lon2: Double, dist: Double, total_dist: Double)

  /**
    *
    * @param trip_id
    * @param stop_id
    * @param sequence
    * @param dist
    */
  case class TripStop(trip_id: Long, stop_id: String, sequence: Int, dist: Double)

  /**
    *
    * @param id
    * @param price
    * @param transfers
    * @param transfer_duration
    */
  case class Fare(id: String, price: Float, transfers: Int, transfer_duration: Int)

  // ------------------- LAT/LON OPERATIONS

  // http://www.movable-type.co.uk/scripts/latlong.html
  // http://www.movable-type.co.uk/scripts/latlong-vectors.html
  // https://en.wikipedia.org/wiki/Great-circle_distance#Vector_version

  val EARTH_RADIUS = 6371000

  case class Vector3D(x: Double, y: Double, z: Double) {
    def plus(v: Vector3D): Vector3D = new Vector3D(x + v.x, y + v.y, z + v.z)
    def minus(v: Vector3D): Vector3D = new Vector3D(x - v.x, y - v.y, z - v.z)
    override def equals (o: Any) = o match {
      case that: Vector3D => {
        val v: Vector3D = o.asInstanceOf[Vector3D]
        x == v.x && y == v.y && z == v.z
      }
      case _ => false
    }

    def angleTo(v: Vector3D, sign: Vector3D = null) = {
      var sin = this.cross(v).norm
      val cos = this.dot(v)
      // use sign as reference to get sign of sin
      if (sign != null && this.cross(v).dot(sign) < 0)  sin = -1 * sin
      Math.atan2(sin,cos)
    }

    def dot(v: Vector3D): Double = x * v.x + y * v.y + z * v.z

    def cross(v: Vector3D): Vector3D = new Vector3D(y * v.z - z * v.y, z * v.x - x * v.z, x * v.y - y * v.x)

    def norm: Double = Math.sqrt(x * x + y * y + z * z)

    def toLatLon(): (Double, Double) = {
      val lat_rad = Math.atan2(z, Math.sqrt(x * x + y * y))
      val lon_rad = Math.atan2(y, x)
      (Math.toDegrees(lat_rad), Math.toDegrees(lon_rad))
    }
  }
  def toVector3D(point: (Double, Double)): Vector3D = {
    val lat = Math.toRadians(point._1)
    val lon = Math.toRadians(point._2)
    // right-handed vector: x -> 0°E,0°N; y -> 90°E,0°N, z -> 90°N
    val x = Math.cos(lat) * Math.cos(lon)
    val y = Math.cos(lat) * Math.sin(lon)
    val z = Math.sin(lat)
    new Vector3D(x, y, z)
  }


  def distance(point1: (Double, Double), point2: (Double, Double)): Double = {
    if (point1._1 == point2._1 && point1._2 == point2._2) return .0
    val a = toVector3D(point1)
    val b = toVector3D(point2)
    EARTH_RADIUS * a.angleTo(b)
  }

  val distanceFunction: (Double, Double, Double, Double) => Double =
    (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
    if (lat1 == lat2 && lon1 == lon2) .0
    else {
      val a = toVector3D((lat1, lon1))
      val b = toVector3D((lat2, lon2))
      EARTH_RADIUS * a.angleTo(b)
    }
  }
  val distanceUDF = functions.udf(distanceFunction)


  def isOnSegment(p0_lat: Double, p0_lon: Double, p1_lat: Double, p1_lon: Double, p2_lat: Double, p2_lon: Double): Boolean = {

    var onSegment = false

    if (p1_lat == p2_lat && p1_lon == p2_lon)
      onSegment = p0_lat == p1_lat && p0_lon == p1_lon
    else {
      // n-vectors
      val n0 = toVector3D((p0_lat, p0_lon))
      val n1 = toVector3D((p1_lat, p1_lon))
      val n2 = toVector3D((p2_lat, p2_lon))
      // get vectors representing p0->p1, p0->p2, p1->p2, p2->p1
      val n10 = n0.minus(n1)
      val n12 = n2.minus(n1)
      val n20 = n0.minus(n2)
      val n21 = n1.minus(n2)

      // dot product p0->p1⋅p2->p1 tells us if p0 is on p2 side of p1, similarly for p0->p2⋅p1->p2
      val extent1 = n10.dot(n12)
      val extent2 = n20.dot(n21)

      onSegment = extent1 >= 0 && extent2 >= 0

      // n12.norm == n21.norm always
      // if n0, n1 and n2 form a triangle, n10.norm+n20.norm > n12.norm
      // if n0, n1 and n2 are collinear but n0 is outside n1->n2, n10.norm+n20.norm > n12.norm
      // if n0 is within n1->n2, n10norm+n20.norm ~== n12.norm

      //      onSegment = Math.abs(n10.norm + n20.norm - n12.norm) < 3e-7

    }
    onSegment
  }

  val isOnSegmentFunction: (Double, Double, Double, Double, Double, Double) => Boolean =
    (lat: Double, lon: Double, lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {

      if (lat1 == lat2 && lon1 == lon2)
        lat1 == lat && lon1 == lon
      else {
        // n-vectors
        val n0 = toVector3D((lat, lon))
        val n1 = toVector3D((lat1, lon1))
        val n2 = toVector3D((lat2, lon2))

        // get vectors representing p0->p1, p2->p1, p0->p2, p1->p2
        val n10 = n0.minus(n1)
        val n12 = n2.minus(n1)
        val n20 = n0.minus(n2)
        val n21 = n1.minus(n2)

        // n12.norm == n21.norm always
        // if n0, n1 and n2 form a triangle, n10.norm+n20.norm > n12.norm
        // if n0, n1 and n2 are collinear but n0 is outside n1->n2, n10.norm+n20.norm > n12.norm
        // if n0 is within n1->n2, n10norm+n20.norm ~== n12.norm

        Math.abs(n10.norm + n20.norm - n12.norm) < 2e-7

      }
    }
  val isOnSegmentUDF = functions.udf(isOnSegmentFunction)

  def nearestPointOnSegment(point: (Double, Double), point1: (Double, Double), point2: (Double, Double)): (Double, Double) = {
    val n0 = toVector3D(point)
    val n1 = toVector3D(point1)
    val n2 = toVector3D(point2)
    val c1 = n1.cross(n2) // n1×n2 = vector representing great circle through p1, p2
    val c2 = n0.cross(c1) // n0×c1 = vector representing great circle through p0 normal to c1
    val n = c1.cross(c2)  // c2×c1 = nearest point on c1 to n0
    n.toLatLon()
  }

  def alongTrackDistance(point: (Double, Double), point1: (Double, Double), point2: (Double, Double)): Double = {
    val pointOnSegment = nearestPointOnSegment(point, point1, point2)
    distance(point1, pointOnSegment)
  }

  val alongTrackDistanceFunction: (Double, Double, Double, Double, Double, Double) => Double =
    (lat: Double, lon: Double, lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
      val n0 = toVector3D((lat,lon))
      val n1 = toVector3D((lat1,lon1))
      val n2 = toVector3D((lat2,lon2))
      val c1 = n1.cross(n2) // n1×n2 = vector representing great circle through p1, p2
      val c2 = n0.cross(c1) // n0×c1 = vector representing great circle through p0 normal to c1
      val n = c1.cross(c2)  // c2×c1 = nearest point on c1 to n0
      EARTH_RADIUS * n0.angleTo(n)
    }
  val alongTrackDistanceUDF = functions.udf(alongTrackDistanceFunction)

  SparkSqlContext.udf.register("alongTrack", alongTrackDistanceFunction)


//   val SparkGlobalContext = sc
//   val SparkSqlContext = sqlContext
//   import org.apache.spark.sql.functions
  // -22.922103514765354,-43.263407553315915
  // -22.921769186398762,-43.26256425004004
  // -22.921091593102517,-43.26088752433975


//  def crossTrackDistance(point: (Double, Double), point1: (Double, Double), point2: (Double, Double)): Double = {
//    if (point1._1 == point2._1 && point1._2 == point2._2)
//      return distance(point, point1)
//    else {
//      val the_point = toVector3D(point)
//      val greatCircle = toVector3D(point1).cross(toVector3D(point2))
//      var angle = greatCircle.angleTo(the_point, the_point.cross(greatCircle)) // (signed) angle between point & great-circle normal vector
//      if (angle < 0) angle = -1 * Math.PI / 2 - angle else Math.PI / 2 - angle // (signed) angle between point & great-circle
//      EARTH_RADIUS * angle // -ve if to left, +ve if to right of path. In this case, we need just abs values
//    }
//  }
//
//  val crossTrackDistanceFunction: (Double, Double, Double, Double, Double, Double) => Double =
//    (lat: Double, lon: Double, lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
//      if (lat1 == lat2 && lon1 == lon2)
//        distance((lat,lon), (lat1,lon1))
//      else {
//        val the_point = toVector3D((lat, lon))
//        val greatCircle = toVector3D((lat1, lon1)).cross(toVector3D((lat2, lon2)))
//        var angle = greatCircle.angleTo(the_point, the_point.cross(greatCircle)) // (signed) angle between point & great-circle normal vector
//        if (angle < 0) angle = -1 * Math.PI / 2 - angle else Math.PI / 2 - angle // (signed) angle between point & great-circle
//        abs(EARTH_RADIUS * angle) // -ve if to left, +ve if to right of path
//      }
//    }
//  val crossTrackDistanceUDF = functions.udf(crossTrackDistanceFunction)



  //  val crossTrackDistanceUDF = functions.udf {
  //    (lat: Double, lon: Double, lat1: Double, lon1: Double, lat2: Double, lon2: Double) =>
  //
  //      val y = sin(lon - lon1) * cos(lat)
  //      val x = cos(lat1) * sin(lat) - sin(lat1) * cos(lat) * cos(lat - lat1)
  //      var bearing1 = toDegrees(atan2(y, x))
  //      bearing1 = 360 - (bearing1 + 360 % 360)
  //
  //      val y2 = sin(lon2 - lon1) * cos(lat2)
  //      val x2 = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(lat2 - lat1)
  //      var bearing2 = toDegrees(atan2(y2, x2))
  //      bearing2 = 360 - (bearing2 + 360 % 360)
  //
  //      val lat1Rads = toRadians(lat1)
  //      val lat3Rads = toRadians(lat)
  //      val dLon = toRadians(lon - lon1)
  //
  //      val distanceAC = acos(sin(lat1Rads) * sin(lat3Rads) + cos(lat1Rads) * cos(lat3Rads) * cos(dLon)) * EARTH_RADIUS
  //
  //      abs(asin(sin(distanceAC / EARTH_RADIUS)) * sin(toRadians(bearing1) - toRadians(bearing2))) * EARTH_RADIUS: Double
  //  }
  //  val alongTrackDistanceUDF = functions.udf {
  //    (lat: Double, lon: Double, lat1: Double, lon1: Double, lat2: Double, lon2: Double) =>
  //
  //      val y = sin(lon - lon1) * cos(lat)
  //      val x = cos(lat1) * sin(lat) - sin(lat1) * cos(lat) * cos(lat - lat1)
  //      var bearing1 = toDegrees(atan2(y, x))
  //      bearing1 = 360 - (bearing1 + 360 % 360)
  //
  //      val y2 = sin(lon2 - lon1) * cos(lat2)
  //      val x2 = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(lat2 - lat1)
  //      var bearing2 = toDegrees(atan2(y2, x2))
  //      bearing2 = 360 - (bearing2 + 360 % 360)
  //
  //      val lat1Rads = toRadians(lat1)
  //      val lat3Rads = toRadians(lat)
  //      val dLon = toRadians(lon - lon1)
  //
  //      val distanceAC = acos(sin(lat1Rads) * sin(lat3Rads) + cos(lat1Rads) * cos(lat3Rads) * cos(dLon)) * EARTH_RADIUS
  //      val crossTrackDistance = abs(asin(sin(distanceAC / EARTH_RADIUS)) * sin(toRadians(bearing1) - toRadians(bearing2))) * EARTH_RADIUS
  //
  //      acos(cos(distanceAC) / cos(crossTrackDistance)) * EARTH_RADIUS: Double
  //
  //  }


  //  def haversineDistance (point1: (Double, Double), point2:(Double, Double)): Double = {
  //    // Haversine distance (in meters)
  //    val dLat = (point2._1 - point1._1).toRadians
  //    val dLon = (point2._2 - point1._2).toRadians
  //    val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(point1._1.toRadians) * cos(point2._1.toRadians)
  //    val c = 2 * asin(sqrt(a))
  //    EARTH_RADIUS * c
  //  }
  //
  //  val haversineDistanceFunction = (point1: (Double, Double), point2:(Double, Double)) => haversineDistance(point1, point2)
  //
  //  val haversineDistanceUDF = functions.udf(haversineDistanceFunction)

  //  def trackDistances (point: (Double, Double), point1: (Double, Double), point2: (Double, Double)): (Double, Double) = {
  //    //http://www.movable-type.co.uk/scripts/latlong.html
  //
  //    val y = sin(point._2 - point1._2) * cos(point._1)
  //    val x = cos(point1._1) * sin(point._1) - sin(point1._1) * cos(point._1) * cos(point._1 - point1._1)
  //    var bearing1 = toDegrees(atan2(y, x))
  //    bearing1 = 360 - (bearing1 + 360 % 360)
  //
  //    val y2 = sin(point2._2 - point1._2) * cos(point2._1)
  //    val x2 = cos(point1._1) * sin(point2._1) - sin(point1._1) * cos(point2._1) * cos(point2._1 - point1._1)
  //    var bearing2 = toDegrees(atan2(y2, x2))
  //    bearing2 = 360 - (bearing2 + 360 % 360)
  //
  //    val lat1Rads = toRadians(point1._1)
  //    val lat3Rads = toRadians(point._1)
  //    val dLon = toRadians(point._2 - point1._2)
  //
  //    val distanceAC = acos(sin(lat1Rads) * sin(lat3Rads)+cos(lat1Rads)*cos(lat3Rads)*cos(dLon)) * EARTH_RADIUS
  //
  //    // Like an orthogonal distance from point to line [point1, point2]
  //    val crossTrackDistance = abs(asin(sin(distanceAC/EARTH_RADIUS))*sin(toRadians(bearing1)-toRadians(bearing2))) * EARTH_RADIUS
  //    // Like an orthogonal projection, distance from point1 to the closest point on the path [point1, point2]
  //    val alongTrackDistance = acos(cos(distanceAC)/cos(crossTrackDistance)) * EARTH_RADIUS
  //
  //
  //    (crossTrackDistance, alongTrackDistance)
  //
  //  }

  //  SparkSqlContext.udf.register("crossTrackDistance", (point_lat: Double, point_lon: Double,
  //                                                      point1_lat: Double, point1_lon: Double,
  //                                                      point2_lat: Double, point2_lon: Double) => {
  //    trackDistances ((point_lat, point_lon), (point1_lat, point1_lon), (point2_lat, point2_lon))._1
  //  })
  //
  //  SparkSqlContext.udf.register("alongTrackDistance", (point_lat: Double, point_lon: Double,
  //                                                      point1_lat: Double, point1_lon: Double,
  //                                                      point2_lat: Double, point2_lon: Double) => {
  //    trackDistances ((point_lat, point_lon), (point1_lat, point1_lon), (point2_lat, point2_lon))._2
  //  })


}
