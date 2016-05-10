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
  val Local_Dir = "file:///home/jonny/Documentos/UFRJ/BigData/RioSmartStops/data/"
  val GTFS_Data_Dir = Local_Dir + "gtfs/"
  val GPS_Data_Dir = Local_Dir + "gps/"
  val Checkpoint_Dir = Local_Dir + "bkp/"
  val GTFS_Fare_Attributes_File = GTFS_Data_Dir + "fare_attributes.txt"
  val GTFS_Fare_Rules_File = GTFS_Data_Dir + "fare_rules.txt"
  val GTFS_Routes_File = GTFS_Data_Dir + "routes.txt"
  val GTFS_Shapes_File = GTFS_Data_Dir + "shapes.txt"
  val GTFS_Stops_File = GTFS_Data_Dir + "stops.txt"
  val GTFS_Stop_Times_File = GTFS_Data_Dir + "stop_times.txt"
  val GTFS_Trips_File = GTFS_Data_Dir + "trips.txt"

  // Filenames to local data saved
  val Bus_GPS_Data_File = GPS_Data_Dir + "onibus.json"
  val BRT_GPS_Data_File = GPS_Data_Dir + "brt.json"
  val Test_GPS_Data_File = GPS_Data_Dir + "test.json"

  // Spark configuration
  val SparkConf = new SparkConf().setAppName("RioSmartStops")
    .setMaster("spark://127.0.0.1:7077")
    .set("spark.driver.memory", "4g")
    .set("spark.executor.memory", "4g")
    .set("spark.cleaner.ttl", "6000")

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

  val DBConnection = new com.mysql.jdbc.Driver

  val GlobalRegex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)".r
  // Splitting with commas but not ones inside quotes
  val BRSRegex = "\\bBRS?\\b (([ABCDI0-9])+, ?)+([A-Za-z0-9 -])+".r

  // Helper classes to read data files and write to RDD and DB schemas
  val EARTH_RADIUS = 6371000
  // Earth radius in m
  val distanceFunction = (point1: (Double, Double), point2: (Double, Double)) => distance(point1, point2)
  val distanceUDF = functions.udf(distanceFunction)
  val crossTrackDistanceUDF = functions.udf {
    (lat: Double, lon: Double, lat1: Double, lon1: Double, lat2: Double, lon2: Double) =>

      val y = sin(lon - lon1) * cos(lat)
      val x = cos(lat1) * sin(lat) - sin(lat1) * cos(lat) * cos(lat - lat1)
      var bearing1 = toDegrees(atan2(y, x))
      bearing1 = 360 - (bearing1 + 360 % 360)

      val y2 = sin(lon2 - lon1) * cos(lat2)
      val x2 = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(lat2 - lat1)
      var bearing2 = toDegrees(atan2(y2, x2))
      bearing2 = 360 - (bearing2 + 360 % 360)

      val lat1Rads = toRadians(lat1)
      val lat3Rads = toRadians(lat)
      val dLon = toRadians(lon - lon1)

      val distanceAC = acos(sin(lat1Rads) * sin(lat3Rads) + cos(lat1Rads) * cos(lat3Rads) * cos(dLon)) * EARTH_RADIUS

      abs(asin(sin(distanceAC / EARTH_RADIUS)) * sin(toRadians(bearing1) - toRadians(bearing2))) * EARTH_RADIUS: Double
  }
  val alongTrackDistanceUDF = functions.udf {
    (lat: Double, lon: Double, lat1: Double, lon1: Double, lat2: Double, lon2: Double) =>

      val y = sin(lon - lon1) * cos(lat)
      val x = cos(lat1) * sin(lat) - sin(lat1) * cos(lat) * cos(lat - lat1)
      var bearing1 = toDegrees(atan2(y, x))
      bearing1 = 360 - (bearing1 + 360 % 360)

      val y2 = sin(lon2 - lon1) * cos(lat2)
      val x2 = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(lat2 - lat1)
      var bearing2 = toDegrees(atan2(y2, x2))
      bearing2 = 360 - (bearing2 + 360 % 360)

      val lat1Rads = toRadians(lat1)
      val lat3Rads = toRadians(lat)
      val dLon = toRadians(lon - lon1)

      val distanceAC = acos(sin(lat1Rads) * sin(lat3Rads) + cos(lat1Rads) * cos(lat3Rads) * cos(dLon)) * EARTH_RADIUS
      val crossTrackDistance = abs(asin(sin(distanceAC / EARTH_RADIUS)) * sin(toRadians(bearing1) - toRadians(bearing2))) * EARTH_RADIUS

      acos(cos(distanceAC) / cos(crossTrackDistance)) * EARTH_RADIUS: Double

  }

  // Format bus line codes (remove "", ".0" suffix and define length >= 3)
  def formatBusLine(id: String): String = {
    var newId = id
    if (id.length <= 2) return ""
    if (id.length <= 4) newId = ("0" * (2 / (id.length - 2))) + newId
    if (id.contains(".")) newId = newId.substring(0, newId.indexOf("."))
    newId
  }

  /*, arrival: DateType, departure: DateType */

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

  //   Sort:
  //   Sorting.quickSort(array)(function)
  //   array.ordered

  def hash(text: String): String = java.security.MessageDigest.getInstance("MD5")
    .digest(text.getBytes()).map(0xFF & _).map {
    "%02x".format(_)
  }.foldLeft("") {
    _ + _
  }

  def distance(point1: (Double, Double), point2: (Double, Double)) = {
    val a = new VectorLatLon(point1._1, point1._2).toVector3D()
    val b = new VectorLatLon(point2._1, point2._2).toVector3D()
    val dot = a.dot(b)
    val cross = a.cross(b)
    val normValue = cross.norm()
    EARTH_RADIUS * Math.toRadians(Math.atan2(normValue, dot))
  }

  def randomDirectionID(): Int = new Random().nextInt(1)

  def euclideanDistance(point1: (Double, Double), point2: (Double, Double)): Double = {
    // Euclidean distance
    val dx = point2._1 - point1._1
    val dy = point2._2 - point1._2
    math.sqrt(dx * dx + dy * dy) * 100000
  }

  // http://www.movable-type.co.uk/scripts/latlong.html
  // http://www.movable-type.co.uk/scripts/latlong-vectors.html
  // https://en.wikipedia.org/wiki/Great-circle_distance#Vector_version


  //  def distance (point1: (Double, Double), point2: (Double, Double)): Double = {
  //    val a = new Vector3D(point1._1, point1._2, 0)
  //    val b = new Vector3D(point2._1, point2._2, 0)
  //    val dot = a.dotProduct(b)
  //    val cross = a.crossProduct(b)
  //    val normValue = cross.getNorm()
  //    EARTH_RADIUS * Math.atan2(normValue,dot)
  //  }

  def projection(point: (Double, Double), point1: (Double, Double), point2: (Double, Double)):
  (Double, Double) = {
    val delta_lat = point2._1 - point1._1
    val delta_lon = point2._2 - point1._2
    val d = ((delta_lat * (point._1 - point1._1)) + (delta_lon * (point._2 - point1._2))) / ((delta_lat * delta_lat) + (delta_lon * delta_lon))
    val proj_lat = point1._1 + delta_lat * d
    val proj_lon = point1._2 + delta_lon * d
    //if (proj_lat < point2._1 && proj_lat > point1._1 && proj_lon < point2._2 && proj_lon > point1._2)
    (proj_lat, proj_lon)
    //(-1, -1)
  }

  /*def exists(dir: String, filename: String): Boolean = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", dir)
    val fs = FileSystem.get(conf)
    val path = new Path(dir + filename)
    fs.exists(path)
  }

  def remove(p: String) {
    val conf = new Configuration()
    conf.set("fs.defaultFS", p)
    val fs = FileSystem.get(conf)
    val path = new Path(p)
    fs.delete(path, true)
    fs.close()
  }

  // Download raw data if boolean is set true
  // and remote and local files are different.

  def GetData(remoteFile: String, localDir: String, localFilename: String): Boolean = {

    if (((remoteFile == GPSRawDataHttpFile) && !getNewGPSData) ||
      ((remoteFile != GPSRawDataHttpFile) && !getNewData)) {
      println("getData = false. I won't download anything")
      return false
    }

    // Get http file and save to hdfs
    val conf = new Configuration()
    conf.set("fs.defaultFS", localDir)

    val fs = FileSystem.get(conf)
    val path = new Path(localDir + localFilename)

    // File not exists => Create new file
    // File exists
    //  |- Hash is different => Create new file
    //  |- Hash is the same => Nothing to do

    // Trying to connect in order to get new GPS raw data
    var source: BufferedSource = null
    try source = Source.fromURL(remoteFile, "ISO-8859-1")

    catch {
      case e: Exception => println("Unable to connect to " + remoteFile)
        return false
    }
    finally

    // If connection was successful, verify hashcodes of local and remote files.
    // If both are different, download new data
      if ((source != null) && !(fs.exists(path) && fs.hashCode() == source.hashCode())) {
        println("Retrieving new data... " + localFilename)
        val os = fs.create(path)
        source.foreach(c => os.write(c.toChar))
        os.write('\n')
        os.flush()
        os.close()
      }
    true
  }

  def openFile(file: String): RDD[String] = {
    val a: RDD[String] =
      try {
        SparkGlobalContext.textFile(BusLinesDir + BusLineStopsFilename(file))
      }
      catch {
        case e: Exception => {
          println("No file.")
          null
        }
      }
    a
  }

*/
  def showAll(df: DataFrame): Unit = {
    df.show(df.count().toInt)
  }

  // Stop Spark context
  def stop() {
    //SparkStreamingContext.stop()
    SparkGlobalContext.stop()
  }

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

  //  val a = distance ((-22.881987092929933, -43.3293562409977), (-22.88157931873053, -43.33008429795194)) =  88m
  //  val b = distance ((-22.890722691798715, -43.45459828068607), (-22.890609220738895, -43.45453864394324)) = 14m

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
                   lat2: Double, lon2: Double, dist: Double)

  //  val crossTrackDistanceUDF = functions.udf {
  //    (lat: Double, lon: Double, lat1: Double, lon1: Double, lat2: Double, lon2: Double) =>
  //      trackDistances ((lat, lon), (lat1, lon1), (lat2, lon2))._1 : Double
  //  }
  //
  //  val alongTrackDistanceUDF = functions.udf {
  //    (lat: Double, lon: Double, lat1: Double, lon1: Double, lat2: Double, lon2: Double) =>
  //      trackDistances ((lat, lon), (lat1, lon1), (lat2, lon2))._2 : Double
  //  }

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

  case class Vector3D(x: Double, y: Double, z: Double) {
    def dot(v: Vector3D): Double = x * v.x + y * v.y + z * v.z

    def cross(v: Vector3D): Vector3D = new Vector3D(y * v.z - z * v.y, z * v.x - x * v.z, x * v.y - y * v.x)

    def norm(): Double = Math.sqrt(x * x + y * y + z * z)

    def toLatLon(): (Double, Double) = (Math.atan2(z, Math.sqrt(x * x + y * y)), Math.atan2(y, x))
  }

  class VectorLatLon(lat: Double, lon: Double) {
    def toVector3D(): Vector3D = {
      val la = Math.toRadians(lat)
      val lo = Math.toRadians(lon)
      val be = Math.toRadians(96)

      val x = Math.sin(lo) * Math.cos(be) - Math.sin(la) * Math.cos(lo) * Math.sin(be)
      val y = -1 * Math.cos(lo) * Math.cos(be) - Math.sin(la) * Math.sin(lo) * Math.sin(be)
      val z = Math.cos(la) * Math.sin(be)
      return new Vector3D(x, y, z)
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
}
