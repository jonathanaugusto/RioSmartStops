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

import RioSmartStops.Global._
import org.apache.spark.rdd._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable._

// Functions to operate and manipulate GPS data.
// Global variables and minor IO/formatting functions
// can be found at Global.scala

object Functions {


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

  def UpdateGTFSData() {

    // GetFaresAndRoutes()
    // GetShapes()
    UpdateStopsAndTrips()
  }


  def GetRoutesDF(route_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading routes table... ")

    val data =
      if (route_id == null)
        SparkSqlContext.read.jdbc(DBConnectionString, "route", DBConnectionProperties)
      else
        SparkSqlContext.read.jdbc(DBConnectionString, "route", DBConnectionProperties)
          .filter(col("id") === route_id)

    if (data.count() == 0) {
      println("No data")
      UpdateFaresAndRoutes()
      return GetRoutesDF(route_id, true)

    }

    val data_arr = data.collect().map(f =>
      Route(id = f(0).toString, code = f(1).toString, name = f(2).toString,
        info = f(3).toString, color = f(4).toString, fare_id = f(5).toString))

    val data_rdd = SparkGlobalContext.broadcast(data_arr)
    val data_df = SparkSqlContext.createDataFrame(data_rdd.value)

    if (debug && !recursive) println("Loaded. Count: " + data_df.count())

    data_df
  }

  def GetFaresDF(fare_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading fares table... ")

    val data =
      if (fare_id == null)
        SparkSqlContext.read.jdbc(DBConnectionString, "fare", DBConnectionProperties)
      else
        SparkSqlContext.read.jdbc(DBConnectionString, "fare", DBConnectionProperties)
          .filter(col("id") === fare_id)

    if (data.count() == 0) {
      println("No data")
      UpdateFaresAndRoutes()
      return GetFaresDF(fare_id, true)

    }

    val data_arr = data.collect().map(f =>
      Fare(id = f(0).toString, price = f(1).toString.toFloat,
        transfers = f(2).toString.toInt, transfer_duration = f(3).toString.toInt))

    val data_rdd = SparkGlobalContext.broadcast(data_arr)
    val data_df = SparkSqlContext.createDataFrame(data_rdd.value)

    if (debug && !recursive) println("Loaded. Count: " + data_df.count())

    data_df
  }

  def GetShapesDF(shape_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading shapes table... ")

    val data =
      if (shape_id == null)
        SparkSqlContext.read.jdbc(DBConnectionString, "shape", DBConnectionProperties)
      else
        SparkSqlContext.read.jdbc(DBConnectionString, "shape", DBConnectionProperties)
          .filter(col("id") === shape_id)

    if (data.count() == 0) {
      println("No data")
      UpdateShapes()
      return GetShapesDF(shape_id, true)

    }

    if (shape_id != null) {
      val data_arr = data.collect().map(f =>
        Shape(id = f(0).toString, sequence = f(1).toString.toLong, lat1 = f(2).toString.toDouble,
          lon1 = f(3).toString.toDouble, lat2 = f(4).toString.toDouble, lon2 = f(5).toString.toDouble,
          dist = f(6).toString.toDouble))

      val data_rdd = SparkGlobalContext.broadcast(data_arr)
      val data_df = SparkSqlContext.createDataFrame(data_rdd.value)

      if (debug && !recursive) println("Loaded. Count: " + data_df.count())

      return data_df
    }

    if (debug && !recursive) println("Loaded. Count: " + data.count() + "\r\n" +
      "ATTENTION: Need to call some ACTION to avoid lazy-transformation problems")

    data
  }

  def GetStopsDF(stop_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading stops table... ")

    val data =
      if (stop_id == null)
        SparkSqlContext.read.jdbc(DBConnectionString, "stop", DBConnectionProperties)
      else
        SparkSqlContext.read.jdbc(DBConnectionString, "stop", DBConnectionProperties)
          .filter(col("id") === stop_id)

    if (data.count() == 0) {
      println("No data")
      UpdateStops()
      return GetStopsDF(stop_id, true)
    }

    val data_arr = data.collect().map(f =>
      Stop(id = f(0).toString, code = f(1).toString, name = f(2).toString,
        desc = f(3).toString, lat = f(4).toString.toDouble, lon = f(5).toString.toDouble))

    val data_rdd = SparkGlobalContext.broadcast(data_arr)
    val data_df = SparkSqlContext.createDataFrame(data_rdd.value)

    if (debug && !recursive) println("Loaded. Count: " + data_df.count())

    data_df
  }

  def GetTripsDF(trip_id: String = null, route_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading trips table... ")

    var data: DataFrame = SparkSqlContext.read.jdbc(DBConnectionString, "trip", DBConnectionProperties)

    if (trip_id != null && route_id != null)
      data = data.filter(col("id") === trip_id && col("route_id") === route_id)
    else if (trip_id == null && route_id != null)
      data = data.filter(col("route_id") === route_id)
    else if (trip_id != null && route_id == null)
      data = data.filter(col("id") === trip_id)

    if (data.count() == 0) {
      println("No data")
      UpdateTrips()
      return GetTripsDF(trip_id, route_id, true)
    }

    val data_arr = data.collect().map(f =>
      Trip(id = f(0).toString.toLong, route_id = f(1).toString, direction = f(2).toString.toInt,
        headsign = f(3).toString, shape_id = f(4).toString))

    val data_rdd = SparkGlobalContext.broadcast(data_arr)
    val data_df = SparkSqlContext.createDataFrame(data_rdd.value)

    if (debug && !recursive) println("Loaded. Count: " + data_df.count())

    data_df
  }

  def UpdateFaresAndRoutes() {

    if (debug) println("Reading previously saved information (fares/routes)")

    // Reading fare and route tables

    val fares_data = SparkSqlContext.read.
      jdbc(DBConnectionString, "fare", DBConnectionProperties)
    val routes_data = SparkSqlContext.read.
      jdbc(DBConnectionString, "route", DBConnectionProperties)

    if (fares_data.count() > 0 && routes_data.count() > 0) {
      if (debug) println("Loaded")
      //      FaresDF = fares_data
      //      RoutesDF = routes_data
      //      FaresDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      //      RoutesDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      return
    }
    else if (debug) println("No data loaded. Generating new tables")

    // --------------------------------------------- fare_attributes.txt

    if (debug) println("Reading fare_attributes.txt")

    val fare_attribs_csv = SparkGlobalContext.textFile(GTFS_Fare_Attributes_File)
    val fare_attribs_header = fare_attribs_csv.first

    val fare_attribs_columns = fare_attribs_csv
      .filter(_ != fare_attribs_header) // Strip header
      .map(_.split(",")) // Split with comma

    val fare_attribs_rdd = fare_attribs_columns
      .map(p => Fare(p(1).drop(1).dropRight(1), p(2).drop(1).dropRight(1).toFloat,
        p(5).drop(1).dropRight(1).toInt, p(6).drop(1).dropRight(1).toInt))

    val fare_attribs_df = SparkSqlContext.createDataFrame(fare_attribs_rdd)

    fare_attribs_csv.unpersist()

    // --------------------------------------------- fare_rules.txt

    if (debug) println("Reading fare_rules.txt")

    val fare_rules_csv = SparkGlobalContext.textFile(GTFS_Fare_Rules_File)
    val fare_rules_header = fare_rules_csv.first

    val fare_rules_rdd = fare_rules_csv
      .filter(_ != fare_rules_header) // Strip header
      .map(_.split(",")) // Split with comma
      .map(f => (f(0).drop(1).dropRight(1), f(1).drop(1).dropRight(1)))

    val fare_rules_df = SparkSqlContext.createDataFrame(fare_rules_rdd).toDF("fare_id", "route_id")
    //    FaresDF = fare_rules_df
    //    FaresDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //fare_rules_df.unpersist()
    fare_rules_csv.unpersist()


    // --------------------------------------------- routes.txt

    if (debug) println("Reading routes.txt")

    val routes_csv = SparkGlobalContext.textFile(GTFS_Routes_File)
    val routes_header = routes_csv.first

    val routes_columnsRDD = routes_csv
      .filter(_ != routes_header) // Strip header
      .map(_.split(",")) // Split with comma
      .map(p => (p(0).drop(1).dropRight(1), p(2).drop(1).dropRight(1), p(3).drop(1).dropRight(1),
      p(4).drop(1).dropRight(1), p(7).drop(1).dropRight(1)))

    val routes_df = SparkSqlContext.createDataFrame(routes_columnsRDD)
      .toDF("route_id", "route_short_name", "route_long_name", "route_desc", "route_color")
      .join(fare_rules_df, "route_id")
      .toDF("id", "code", "name", "info", "color", "fare_id")
    //    RoutesDF = routes_df
    //    RoutesDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

    fare_attribs_df.write.mode("append").jdbc(DBConnectionString, "fare", DBConnectionProperties)
    routes_df.write.mode("append").jdbc(DBConnectionString, "route", DBConnectionProperties)

    fare_attribs_df.unpersist()
    fare_rules_df.unpersist()
    routes_df.unpersist()
    routes_csv.unpersist()
  }

  def UpdateShapes() {

    if (debug) println("Reading shapes.txt -- sequentially, in order to calculate travelled distances")

    // --------------------------------------------- shapes.txt

    val shapes_csv = SparkGlobalContext.textFile(GTFS_Shapes_File)
    val shapes_header = shapes_csv.first

    // RDD of Shapes partitioned by groups with same shape_id
    val shapes_grouped_rdd: RDD[(String, (String, Int, Double, Double))]
    = shapes_csv
      .filter(_ != shapes_header) // Strip header
      .map(_.split(",")) // Split with comma

      .map(f => (f(0).drop(1).dropRight(1), // shape_id
      f(3).drop(1).dropRight(1).toInt, // sequence
      f(1).drop(1).dropRight(1).toDouble, // shape_pt_lat
      f(2).drop(1).dropRight(1).toDouble)) // shape_pt_lon

      .sortBy(f => f._1).keyBy(f => f._1) // Order and group by shape_id

    var shapes_calculated = new ArrayBuffer[Shape]()

    val shapes_ids = shapes_grouped_rdd.keys.distinct().collect()

    for (id <- shapes_ids) {
      var distance: Double = .0
      // Partition group with same shape_id, can be seeked by iterator
      val shape_RDD: RDD[(String, Int, Double, Double)] = shapes_grouped_rdd.filter(f => f._1 == id).map(f => f._2)
      val shape_tuples: List[Array[(String, Int, Double, Double)]] = shape_RDD.collect().sliding(2).toList

      shape_tuples.foreach {
        // Seeking through size-2 sequencies, stepping 1 by 1 element

        l: Array[(String, Int, Double, Double)] =>
          val shape1 = l(0)
          val shape2 = l(1)
          val shape_id = shape1._1
          val sequence = shape1._2
          val lat1 = shape1._3
          val lon1 = shape1._4
          val lat2 = shape2._3
          val lon2 = shape2._4

          distance += distanceFunction((lat1, lon1), (lat2, lon2))
          shapes_calculated += Shape(shape_id, sequence, lat1, lon1, lat2, lon2, distance)

      }

      val shapes_rdd = SparkGlobalContext.parallelize(shapes_calculated)
      val shapes_df = SparkSqlContext.createDataFrame(shapes_rdd)
      shapes_df.write.mode("append").jdbc(DBConnectionString, "shape", DBConnectionProperties)
      shapes_calculated = new ArrayBuffer[Shape]()
      //        ShapesDF = shapes_df
      //        ShapesDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      //      shapes_df.unpersist()
      //      shapes_rdd.unpersist()
    }

  }

  def UpdateStops() = {

    if (debug) println("Reading stops.txt")

    val stops_csv = SparkGlobalContext.textFile(GTFS_Stops_File)
    val stops_header = stops_csv.first

    val stops_columns: RDD[Array[String]] = stops_csv
      .filter(_ != stops_header) // Strip header
      .map(_.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) // Split with comma but not inside quotes

    stops_csv.unpersist()

    val stops_rdd: RDD[Stop] = stops_columns
      .map(p => {
        Stop(p(0).drop(1).dropRight(1),
          if (p(1).nonEmpty && p(1).length <= 20) p(1).drop(1).dropRight(1) else "",
          p(2).drop(1).dropRight(1),
          if (p(3).nonEmpty) p(3).drop(1).dropRight(1) else "",
          p(4).drop(1).dropRight(1).toDouble,
          p(5).drop(1).dropRight(1).toDouble)
      }
      )

    val stops_df = SparkSqlContext.createDataFrame(stops_rdd).orderBy("id")

    println("Loaded. Count: " + stops_df.count())
    //      StopsDF = stops_df
    //      StopsDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    stops_rdd.unpersist()

    //    println("count = " + stops_df.count())
    stops_df.printSchema()
    //    stops_df.show(stops_df.count().toInt)

    stops_df.write.mode("append").jdbc(DBConnectionString, "stop", DBConnectionProperties)

  }

  def UpdateTrips() = {

    if (debug) println("Reading trips.txt")

    val trips_csv = SparkGlobalContext.textFile(GTFS_Trips_File)
    val trips_header = trips_csv.first

    val trips_columns_rdd = trips_csv
      .filter(_ != trips_header) // Strip header
      .map(_.split(",")) // Split with comma
      .map(p =>
      (p(2).drop(1).dropRight(1).toLong: Long, // Trip ID (Momentarily Long)
        //p(1).drop(1).dropRight(1): String,        // Service ID
        p(0).drop(1).dropRight(1): String, // Route ID
        p(4).drop(1).dropRight(1).toInt: Int, // Trip direction ID
        p(3).drop(1).dropRight(1): String, // Headsign
        p(6).drop(1).dropRight(1): String)) // Shape ID

    val trips_columns_df = SparkSqlContext.createDataFrame(trips_columns_rdd)
      .toDF("id", /*"service_id",*/ "route_id", "direction", "headsign", "shape_id")

    // Order by Route ID, Select the minimum Trip ID (saving just one of them: the minimum ID)
    val trips_routes_simplified = trips_columns_df.groupBy("route_id")
      .agg("id" -> "min").orderBy("min(id)").toDF("route", "min")

    val trips_rdd = trips_routes_simplified
      .join(trips_columns_df, col("min") === col("id"), "left_outer")
      .drop("route").drop("min")
      .map(f => Trip(f(0).toString.toLong, f(1).toString, f(2).toString.toInt, f(3).toString, f(4).toString))

    trips_routes_simplified.unpersist()
    trips_columns_df.unpersist()

    val trips_df = SparkSqlContext.createDataFrame(trips_rdd)

    println("Loaded. Count: " + trips_df.count())
    //      TripsDF = trips_df
    //      TripsDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    trips_df.printSchema()
    //    trips_df.show()

    trips_df.write.mode("append").jdbc(DBConnectionString, "trip", DBConnectionProperties)

  }

  def UpdateStopsAndTrips() {

    if (debug) println("Reading previously saved information (stops/trips)")

    // --------------------------------------------- stop_times.txt

    val tripstops_data = SparkSqlContext.read.
      jdbc(DBConnectionString, "tripstop", DBConnectionProperties)

    if (tripstops_data.count() > 0) {
      println("Relationship data OK. Count: " + tripstops_data.count())
      //      TripStopsDF = tripstops_data
      //      TripStopsDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    }

    else {

      val trips_df = GetTripsDF()

      val stops_df = GetStopsDF()

      if (debug) println("Reading stop_times.txt")

      val stop_times_csv = SparkGlobalContext.textFile(GTFS_Stop_Times_File)
      val stop_times_header = stop_times_csv.first

      val stop_times_txt_rdd: RDD[(Long, String, Int)] = stop_times_csv
        .filter(_ != stop_times_header) // Strip header
        .map(_.split(",")) // Split with comma
        .map(p => (p(0).drop(1).dropRight(1).toLong: Long,
        p(3).drop(1).dropRight(1): String,
        p(4).drop(1).dropRight(1).toInt: Int))
      println("Read. Count: " + stop_times_txt_rdd.count())

      val trip_ids = trips_df.select("id").distinct().map(f => f.toString.drop(1).dropRight(1).toLong).collect()

      // Creating relationship trip_id <=> stop_id
      val stop_times_df = SparkSqlContext.createDataFrame(stop_times_txt_rdd).toDF("trip_id", "stop_id", "sequence")
        .join(trips_df.select("id").distinct().withColumnRenamed("id", "trip_id"), "trip_id")
      //        .collect().map( f => (
      //          f(0).toString.toLong : Long,  // trip_id
      //          f(1).toString        : String,// stop_id
      //          f(2).toString.toInt  : Int    // sequence
      //      ))
      //
      //      val stop_times_rdd = SparkGlobalContext.broadcast(stop_times_arr)
      //      val stop_times_df = SparkSqlContext.createDataFrame(stop_times_rdd.value).toDF("trip_id", "stop_id", "sequence")

      println("Stop times OK. Count: " + stop_times_df.count())
      // 21590

      stop_times_txt_rdd.unpersist()

      if (debug) println("Calculating stop distances by trip_id. Loop: ")

      for (trip_id: Long <- trip_ids) {

        if (debug) println("| trip_id = " + trip_id)

        // Finding shape_id
        val shape_id = trips_df.filter(col("id") === trip_id).select("shape_id")
          .first().toString().drop(1).dropRight(1)
        if (debug) println("| shape_id = " + shape_id)

        // Create DF of shapes collected (filtered too)
        val shapes_df = GetShapesDF(shape_id)
        //        shapes_df.printSchema() // id,sequence,lat1,lon1,lat2,lon2,dist

        // Filtering and collecting relationship trip<=>stop by trip_id
        val df1_data = stop_times_df.filter(stop_times_df("trip_id") === trip_id).drop("trip_id")
          .join(stops_df.select("id", "lat", "lon").withColumnRenamed("id", "stop_id"), "stop_id")
          .collect().map(f => (
          f(0).toString: String, // stop_id
          f(1).toString.toInt: Int, // sequence
          f(2).toString.toDouble: Double, // lat
          f(3).toString.toDouble: Double // lon
          ))
        val df1_rdd = SparkGlobalContext.broadcast(df1_data)
        val df1 = SparkSqlContext.createDataFrame(df1_rdd.value).toDF("stop_id", "sequence", "lat", "lon")
        if (debug) println("| Step 1. Count: " + df1.count())
        //        df1.printSchema() // stop_id,sequence,lat,lon
        //        df1.show(10)

        // Join with all shape points
        val df2 = df1.join(shapes_df.drop("id").drop("sequence"))
        if (debug) println("| Step 2. Count: " + df2.count())
        //               df2.printSchema() // stop_id,sequence,lat,lon,lat1,lon1,lat2,lon2,dist
        //               df2.show(10)

        // Calculate cross-track distance
        val df3_data = df2
          .withColumn("approx",
            crossTrackDistanceUDF(col("lat"), col("lon"), col("lat1"), col("lon1"), col("lat2"), col("lon2")))
          .collect().map(f => (
          f(0).toString: String, // stop_id
          f(1).toString.toInt: Int, // sequence
          f(2).toString.toDouble: Double, // lat
          f(3).toString.toDouble: Double, // lon
          f(4).toString.toDouble: Double, // lat1
          f(5).toString.toDouble: Double, // lon1
          f(6).toString.toDouble: Double, // lat2
          f(7).toString.toDouble: Double, // lon2
          f(8).toString.toDouble: Double, // dist
          f(9).toString.toDouble: Double // approx
          ))
        val df3_rdd = SparkGlobalContext.broadcast(df3_data)
        val df3 = SparkSqlContext.createDataFrame(df3_rdd.value)
          .toDF("stop_id", "sequence", "lat", "lon", "lat1", "lon1", "lat2", "lon2", "dist", "approx")
        if (debug) println("| Step 3. Count: " + df3.count())
        df3.printSchema() // stop_id,sequence,lat,lon,lat1,lon1,lat2,lon2,dist,approx
        //        println(df3.queryExecution)
        df3.show(10)

        // TODO: A partir daqui tá travando o job. Otimizar!
        // Find minimal cross-track distance (groupBy dropping other columns)
        val df4 = df3.groupBy("stop_id", "lat", "lon", "sequence").min("approx")
        if (debug) println("| Step 4. Count: " + df4.count())
        df4.printSchema() // stop_id,lat,lon,sequence,min_approx
        df4.show()

        // Re-joining to recover along-track distance
        val df5 = df4
          .join(df3.drop("approx"), Seq("stop_id", "lat", "lon", "sequence"), "left_outer")
          .filter(col("min_approx").isNotNull) // stop_id,lat,lon,sequence,min(approx),lat1,lon1,lat2,lon2,dist
        df5.printSchema()
        df5.show()
        if (debug) println("| Step 5. Count: " + df5.count())

        val df6 = df5
          .withColumn("alongTrackDistance",
            col("dist") - alongTrackDistanceUDF(col("lat"), col("lon"), col("lat1"), col("lon1"), col("lat2"), col("lon2")))
          .drop("dist")
          .withColumnRenamed("alongTrackDistance", "dist")
          .select("stop_id", "sequence", "dist")
        println("| Step 6. Count: " + df6.count)
        df6.printSchema() // stop_id,sequence,dist
        df6.show()
        //        df5.write.mode("append").jdbc(DBConnectionString, "tripstop_", DBConnectionProperties)

        println("---")
        //        val df8 = df7
        //          .withColumn("alongTrackDistance",
        //            col("dist") + alongTrackDistanceUDF(
        //              df7.col("lat"), df7.col("lon"),
        //              df7.col("lat1"),df7.col("lon1"),
        //              df7.col("lat2"),df7.col("lon2")))
        //          .select("stop_id", "trip_id", "sequence", "alongTrackDistance")
        //          .withColumnRenamed("alongTrackDistance", "dist")

        //        df8.printSchema()
        //        println("df8: " + df8.count())
        //                df8.orderBy("sequence").show(10)
        //        df5.write.mode("append").jdbc(DBConnectionString, "tripstop", DBConnectionProperties)


      }


      // TODO: 1) Manter trips, stops e shapes na memória, não tem jeito. PElo menos pra calcular distância de tripstop
      // TODO: 2) Para todo trip: select mínimo cross-track e calcular ponto1 da reta + along-track = tripstop dist
      // TODO: 3) Guardar em tripstop


      // TODO: Prova final é verificar se ordenando por shape-sequence o stop-sequence continua ordenado (ou vice-versa)


      //        val a = df3.queryExecution.toString
      //        println(a)
      //
      //        // Creating another dataframe
      //        val newdata = df3.map(f => (
      //          f(0) .toString.toLong   : Long,   // trip_id
      //          f(1) .toString          : String, // stop_id
      //          f(3) .toString.toDouble : Double, // lat
      //          f(4) .toString.toDouble : Double, // lon,
      //          f(2) .toString.toLong   : Long,   // sequence,
      //          f(6) .toString.toDouble : Double, // lat1
      //          f(7) .toString.toDouble : Double, // lon1
      //          f(8) .toString.toDouble : Double, // lat2
      //          f(9) .toString.toDouble : Double, // lon2
      //          f(10).toString.toDouble : Double, // dist
      //          f(11).toString.toDouble : Double  // approx
      //          )).collect()
      ////        println("newdata: " + newdata.length)
      //
      //        val newrdd = SparkGlobalContext.parallelize(newdata)
      //        val newdf = SparkSqlContext.createDataFrame(newrdd)
      //          .toDF("trip_id","stop_id","lat","lon","sequence","lat1","lon1","lat2","lon2","dist")
      //        //        newdf.show()


      //    for (id <- trip_ids) {
      //      val trip_stop_columns_df = stop_times_df.where(col("trip_id").===(id)).join(stops_df, "stop_id")
      //      trip_stop_columns_df.printSchema()
      //    }


      //val tripstops_df = SparkSqlContext.createDataFrame(tripstops_rdd)

    }

  }

  //
  //  def FindNearBuses(lat: Double, lon: Double, radius: Double) {
  //
  //
  //    val df2 = SparkSqlContext.sql("SELECT datetime,busId,line FROM GPSData WHERE distance(lat,lon,"
  //      + lat.toString + "," + lon.toString + ") <= " + radius.toString + " ORDER BY datetime DESC")
  //    df2.registerTempTable("NextBuses")
  //
  //    println(df2.count() + " buses found")
  //    showAll(df2)
  //
  //  }
}


// TODO: Fazer uma função de atualização inicial da base pegando linhas do GPS. Mas, também, fazer função de atualização contínua do GPS com SparkStreamingContext; se não achar a linha na base de linhas, chamar as funções de atualização das bases

