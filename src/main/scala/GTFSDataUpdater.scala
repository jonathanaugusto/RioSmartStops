package RioSmartStops

import RioSmartStops.Global._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import scala.collection.mutable._

/**
  * UFRJ - Escola Politécnica
  * "Big Data" course
  * Professor: Villas Boas, Sergio B. [sbVB]
  * Student: Jonathan Augusto <jonathanaugusto@poli.ufrj.br>
  * Project: Default (Template) Project
  * Date: 28/06/16
  */
// GPS Data Updater Program
object GTFSDataUpdater {

  val sc = SparkContext.getOrCreate(SparkConf.setAppName("RioSmartStops-GTFS"))
  val sqlc = SQLContext.getOrCreate(sc)

  sqlc.udf.register("onSegment", isOnSegmentFunction)
  sqlc.udf.register("alongTrack", alongTrackDistanceFunction)

  def main(args: Array[String]) {

    if (args.isEmpty) {
      println("No arg specified. Usage: --routes, --shapes, --trips, --stops or --all")
      return
    }
    var routesOption: Boolean = false
    var shapesOption: Boolean = false
    var tripsOption: Boolean = false
    var stopsOption: Boolean = false
    var allOption: Boolean = false

    args.toList.collect {
      case "--routes" => routesOption = true
      case "--shapes" => shapesOption = true
      case "--trips"  => tripsOption = true
      case "--stops"  => stopsOption = true
      case "--all"    => allOption = true
    }
    if (routesOption || allOption) UpdateFaresAndRoutes()
    if (shapesOption || allOption) UpdateShapes()
    if (stopsOption || allOption) UpdateStops()
    if (tripsOption || allOption) UpdateTrips()
    if (tripsOption || stopsOption || allOption) UpdateTripStops()
//    UpdateDB()

  }

  def GetFaresDF(fare_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading fares table... ")

    var data: DataFrame = sqlc.read.parquet(Parquet_Fares_File)

    if (data.count() == 0) {
      println("No data")
      UpdateFaresAndRoutes()
      return GetFaresDF(fare_id, true)

    }

    if (fare_id == null)
      data = data.filter(data("id") === fare_id)

    //    val data_arr = data.collect().map(f =>
    //      Fare(id = f(0).toString, price = f(1).toString.toFloat,
    //        transfers = f(2).toString.toInt, transfer_duration = f(3).toString.toInt))
    //
    //    val data_rdd = sc.broadcast(data_arr)
    //    val data_df = sqlc.createDataFrame(data_rdd.value)

    if (debug && !recursive) println("Loaded. Count: " + data.count())

    data
  }

  def GetRoutesDF(route_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading routes table... ")

    var data = sqlc.read.parquet(Parquet_Routes_File)

    if (data.count() == 0) {
      println("No data")
      UpdateFaresAndRoutes()
      return GetRoutesDF(route_id, true)

    }

    if (route_id != null)
      data = data.filter(data("id") === route_id)

    //    val data_arr = data.collect().map(f =>
    //      Route(id = f(0).toString, code = f(1).toString, name = f(2).toString,
    //        info = f(3).toString, color = f(4).toString, fare_id = f(5).toString))
    //
    //    val data_rdd = sc.broadcast(data_arr)
    //    val data_df = sqlc.createDataFrame(data_rdd.value)

    if (debug && !recursive) println("Loaded. Count: " + data.count())

    data
  }

  def GetShapesDF(shape_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading shapes table... ")

    var data = sqlc.read.parquet(Parquet_Shapes_File)

    if (data.count() == 0) {
      println("No data")
      UpdateShapes()
      return GetShapesDF(shape_id, true)

    }

    if (shape_id != null)
      data = data.filter(data("id") === shape_id)

    //val data_brd = sc.parallelize(data)
    if (debug && !recursive) println("Loaded. Count: " + data.count() + "\r\n" +
      "ATTENTION: Need to call some ACTION to avoid lazy-transformation problems")

    data
  }

  def GetStopsDF(stop_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading stops table... ")

    var data = sqlc.read.parquet(Parquet_Stops_File)

    if (data.count() == 0) {
      println("No data")
      UpdateStops()
      return GetStopsDF(stop_id, true)
    }

    if (stop_id != null)
      data = data.filter(data("id") === stop_id)

    //    val data_brd = sc.broadcast(data)

    if (debug && !recursive) println("Loaded. Count: " + data.count())

    data
  }

  def GetTripsDF(trip_id: String = null, route_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading trips table... ")

    var data: DataFrame = sqlc.read.parquet(Parquet_Trips_File)

    if (data.count() == 0) {
      println("No data")
      UpdateTrips()
      return GetTripsDF(trip_id, route_id, true)}

    if (trip_id != null && route_id != null)
      data = data.filter(data("id") === trip_id && data("route_id") === route_id)
    else if (trip_id == null && route_id != null)
      data = data.filter(data("route_id") === route_id)
    else if (trip_id != null && route_id == null)
      data = data.filter(data("id") === trip_id)

    //    val data_brd = sc.broadcast(data)

    if (debug && !recursive) println("Loaded. Count: " + data.count())

    data
  }

  def UpdateFaresAndRoutes() {

    if (debug) println("--- UPDATE FARES/ROUTES")

    // --------------------------------------------- fare_attributes.txt

    if (debug) println("Reading fare_attributes.txt")

    val fare_attribs_csv = sc.textFile(GTFS_Fare_Attributes_File)
    val fare_attribs_header = fare_attribs_csv.first

    val fare_attribs_columns = fare_attribs_csv
      .filter(_ != fare_attribs_header) // Strip header
      .map(_.split(",")) // Split with comma

    val fare_attribs_rdd = fare_attribs_columns
      .map(p => Fare(p(1).drop(1).dropRight(1), p(2).drop(1).dropRight(1).toFloat,
        p(5).drop(1).dropRight(1).toInt, p(6).drop(1).dropRight(1).toInt))

    val fare_attribs_df = sqlc.createDataFrame(fare_attribs_rdd)

    fare_attribs_csv.unpersist()

    // --------------------------------------------- fare_rules.txt

    if (debug) println("Reading fare_rules.txt")

    val fare_rules_csv = sc.textFile(GTFS_Fare_Rules_File)
    val fare_rules_header = fare_rules_csv.first

    val fare_rules_rdd = fare_rules_csv
      .filter(_ != fare_rules_header) // Strip header
      .map(_.split(",")) // Split with comma
      .map(f => (f(0).drop(1).dropRight(1), f(1).drop(1).dropRight(1)))

    val fare_rules_df = sqlc.createDataFrame(fare_rules_rdd).toDF("fare_id", "route_id")
    fare_rules_csv.unpersist()


    // --------------------------------------------- routes.txt

    if (debug) println("Reading routes.txt")

    val routes_csv = sc.textFile(GTFS_Routes_File)
    val routes_header = routes_csv.first

    val routes_columnsRDD = routes_csv
      .filter(_ != routes_header) // Strip header
      .map(_.split(",")) // Split with comma
      .map(p => (
        p(0).drop(1).dropRight(1), // route_id
        p(2).drop(1).dropRight(1), // code
        p(3).drop(1).dropRight(1), // name
        p(4).drop(1).dropRight(1), // description
        p(3).drop(1).dropRight(1).split("-")(0).trim, // dest1
        p(3).drop(1).dropRight(1).split("-")(2).trim, // dest2
        p(7).drop(1).dropRight(1)  // color
      ))

    val routes_df = sqlc.createDataFrame(routes_columnsRDD)
      .toDF("route_id", "route_short_name", "route_long_name", "route_desc", "route_dest1", "route_dest2", "route_color")
      .join(fare_rules_df, "route_id")
      .toDF("id", "code", "name", "description", "dest1", "dest2", "color", "fare_id")

    routes_df.show()

    fare_attribs_df.write.mode(SaveMode.Overwrite).parquet(Parquet_Fares_File)
//    fare_attribs_df.write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"fare",DBConnectionProperties)
    routes_df.write.mode(SaveMode.Overwrite).parquet(Parquet_Routes_File)
//    routes_df.write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"route",DBConnectionProperties)

    fare_attribs_df.unpersist()
    fare_rules_df.unpersist()
    routes_df.unpersist()
    routes_csv.unpersist()
  }

  def UpdateShapes() {

    if (debug) println("--- UPDATE SHAPES")

    // Erasing existent file

    val ss = new ArrayBuffer[Shape]() += new Shape("", 0, .0, .0, .0, .0, .0, .0)
    val r = sc.parallelize(ss)
    val d = sqlc.createDataFrame(r)
    d.write.mode(SaveMode.Overwrite).parquet(Parquet_Shapes_File)
//    d.write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"shape",DBConnectionProperties)

    if (debug) println("Reading shapes.txt -- sequentially, in order to calculate travelled distances")

    // --------------------------------------------- shapes.txt

    val shapes_csv = sc.textFile(GTFS_Shapes_File)
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
    var shapes_count = 0

//    print("ids: ")
    for (id <- shapes_ids) {
      shapes_count += 1
      var total_dist: Double = .0
      var sequence_count: Long = 0
      // Partition group with same shape_id, can be seeked by iterator
      val shape_RDD: RDD[(String, Int, Double, Double)] = shapes_grouped_rdd.filter(f => f._1 == id).map(f => f._2)
      val shape_tuples: List[Array[(String, Int, Double, Double)]] = shape_RDD.collect().sliding(2).toList
      shape_tuples.foreach {
        // Seeking through size-2 sequencies, stepping 1 by 1 element

        l: Array[(String, Int, Double, Double)] =>
          val shape1 = l(0)
          val shape2 = l(1)
          val shape_id = shape1._1
          //val sequence = shape1._2
          val lat1 = shape1._3
          val lon1 = shape1._4
          val lat2 = shape2._3
          val lon2 = shape2._4

          if (lat1 != lat2 && lon1 != lon2) {
            val dist = distance((lat1, lon1), (lat2, lon2))
            total_dist += dist
            shapes_calculated += Shape(shape_id, sequence_count, lat1, lon1, lat2, lon2, dist, total_dist)
            sequence_count += 1
          }
      }

      val shapes_rdd = sc.parallelize(shapes_calculated)
      val shapes_df = sqlc.createDataFrame(shapes_rdd)
      shapes_df.write.mode(SaveMode.Append).parquet(Parquet_Shapes_File)
//      shapes_df.write.mode(SaveMode.Append).jdbc(DBConnectionString,"shape",DBConnectionProperties)
      shapes_calculated = new ArrayBuffer[Shape]()

//      if (debug) print(id + " | ")
    }
  }

  def UpdateStops() = {

    if (debug) println("--- UPDATE STOPS")

    if (debug) println("Reading stops.txt")

    val stops_csv = sc.textFile(GTFS_Stops_File)
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
    val stops_df = sqlc.createDataFrame(stops_rdd)//.orderBy("id")
    println("Loaded. Count: " + stops_df.count())

    stops_df.write.mode(SaveMode.Overwrite).parquet(Parquet_Stops_File)
//    stops_df.write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"stop",DBConnectionProperties)

    stops_rdd.unpersist()
  }

  def UpdateTrips() = {

    if (debug) println("--- UPDATE TRIPS")

    if (debug) println("Reading trips.txt")

    val trips_csv = sc.textFile(GTFS_Trips_File)
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

    val trips_columns_df = sqlc.createDataFrame(trips_columns_rdd)
      .toDF("id", /*"service_id",*/ "route_id", "direction", "headsign", "shape_id")

    // Order by Route ID, Select the minimum Trip ID (saving just one of them: the minimum ID)
    val trips_routes_simplified = trips_columns_df.groupBy("route_id")
      .agg("id" -> "min").orderBy("min(id)").toDF("route", "min")

    val trips_rdd = trips_routes_simplified
      .join(trips_columns_df, trips_routes_simplified("min") === trips_columns_df("id"), "left_outer")
      .drop("route").drop("min")
      .map(f => Trip(f(0).toString.toLong, f(1).toString, f(2).toString.toInt, f(3).toString, f(4).toString))

    trips_routes_simplified.unpersist()
    trips_columns_df.unpersist()

    val trips_df = sqlc.createDataFrame(trips_rdd)

    println("Loaded. Count: " + trips_df.count())

    trips_df.write.mode(SaveMode.Overwrite).parquet(Parquet_Trips_File)
//    trips_df.write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"trip",DBConnectionProperties)

  }

  def UpdateTripStops() {

    if (debug) println("--- UPDATE TRIPSTOPS")

//    // Erasing existent file
//
//    val ss = new ArrayBuffer[TripStop]() += new TripStop(0, null, 0, .0)
//    val r = sc.parallelize(ss)
//    val d = sqlc.createDataFrame(r)
//    d.write.mode(SaveMode.Overwrite).parquet(Parquet_TripStops_File)
//    d.write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"tripstop",DBConnectionProperties)

    // --------------------------------------------- stop_times.txt

    val trips_df = GetTripsDF().select("id", "shape_id").withColumnRenamed("id", "trip_id")

    val stops_df = GetStopsDF()

    val shapes_df = GetShapesDF().drop("sequence")

    if (debug) println("Reading stop_times.txt")

    val stop_times_csv = sc.textFile(GTFS_Stop_Times_File)
    val stop_times_header = stop_times_csv.first

    val stop_times_txt_rdd: RDD[(Long, String, Int)] = stop_times_csv
      .mapPartitions(_
        .filter(_ != stop_times_header) // Strip header
        .map(_.split(",")) // Split with comma
        .map(p =>
        (p(0).drop(1).dropRight(1).toLong: Long, // trip_id
          p(3).drop(1).dropRight(1): String, // stop_id
          p(4).drop(1).dropRight(1).toInt: Int)) // sequence
      )
    println("Read. Count: " + stop_times_txt_rdd.count())

    val stop_times_df = sqlc.createDataFrame(stop_times_txt_rdd)
      .toDF("trip_id", "stop_id", "sequence").join(trips_df, "trip_id")
    println("Filtered. Count: " + stop_times_df.count())
    //      stop_times_df.printSchema() // trip_id, stop_id, sequence(stop), shape_id
    // 21590
    stop_times_df.write.format("parquet").mode(SaveMode.Overwrite).save(Parquet_StopTimes_File)

    stop_times_txt_rdd.unpersist()

    if (debug) println("Calculating stop distances by trip_id.")

    //    val trip_ids_data = trips_df.select("trip_id", "shape_id").collect().map(f => (f(0).asInstanceOf[Long], f(1).asInstanceOf[String]))
    //
    //    trip_ids_data.foreach { pair => {
    //
    //      println("-----")
    //      val trip_id = pair._1
    //        if (debug) println("| trip_id = " + trip_id)
    //
    //        // Finding shape_id
    //        val shape_id = pair._2
    //        if (debug) println("| shape_id = " + shape_id)
    //
    //        // Create DF of shapes collected (filtered too)
    //        val shapes_f_df = shapes_df.filter(col("id") === shape_id)

    if (debug) print("Steps OK: ")
    // Filtering and collecting relationship trip<=>stop by trip_id
    val df1 = stop_times_df//.filter(col("trip_id") === trip_id).drop("trip_id")
      .join(stops_df.select("id", "lat", "lon").withColumnRenamed("id", "stop_id"), "stop_id")
    //.drop("shape_id")
    if (debug) print("1 ")

    // Join with all shape points
    val df2 = df1.join(shapes_df, df1("shape_id") === shapes_df("id")) // shapes_f_df
    //      df2.write.mode(SaveMode.Overwrite).parquet(HDFS_Dir + Parquet_Data + "temp.parquet")
    shapes_df.unpersist()
    df1.unpersist()
    df2.unpersist()
    if (debug) print("2 ")

    // Calculate cross-track distance
    val df3a = df2 //sqlc.read.parquet(HDFS_Dir + Parquet_Data + "temp.parquet")
    //          .withColumn("onSegment", functions.callUDF("onSegment", col("lat"), col("lon"), col("lat1"), col("lon1"), col("lat2"), col("lon2")))
    df3a.registerTempTable("df3a")
    val df3 = sqlc.sql("select * from df3a where onSegment(lat,lon,lat1,lon1,lat2,lon2) = true")
    //        df3.write.format("parquet").mode(SaveMode.Overwrite).save(Local_Dir + GTFS_Data + "df3.parquet")
    //        df3.unpersist()
    if (debug) print("3 ")

    val df4 = df3 //sqlc.read.parquet(Local_Dir + GTFS_Data + "df3.parquet")
      //          .filter(col("onSegment") === lit(true))
      .withColumn("alongTrackDistance",
      functions.callUDF("alongTrack", df3("lat"), df3("lon"), df3("lat1"), df3("lon1"), df3("lat2"), df3("lon2")))
    //        df4.write.format("parquet").mode(SaveMode.Overwrite).save(Local_Dir + GTFS_Data + "df4.parquet")
    //        df4.unpersist()
    if (debug) print("4 ")

    val df5 = df4 //sqlc.read.parquet(Local_Dir + GTFS_Data + "df4.parquet")
      .withColumn("stop_dist", df4("total_dist") - df4("dist") + df4("alongTrackDistance"))
    //        df5.write.format("parquet").mode(SaveMode.Overwrite).save(Local_Dir + GTFS_Data + "df5.parquet")
    //        df5.unpersist()
    if (debug) print("5 ")

    val df6 = df5 //sqlc.read.parquet(Local_Dir + GTFS_Data + "df5.parquet")
      .drop("dist")
      .withColumnRenamed("stop_dist", "dist")
      //.withColumn("trip_id", lit(trip_id))
      .select("trip_id", "stop_id", "sequence", "dist")
    if (debug) print("6 ")

    df6.write.mode(SaveMode.Overwrite).parquet(Parquet_TripStops_File)
//    df6.write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"tripstop",DBConnectionProperties)
    if (debug) println("DB.")

    df3a.unpersist()
    df3.unpersist()
    df4.unpersist()
    df5.unpersist()

  }

//  def UpdateDB() {
//    sqlc.read.parquet(Parquet_Fares_File).write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"fare",DBConnectionProperties)
//    sqlc.read.parquet(Parquet_Routes_File).write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"route",DBConnectionProperties)
//    sqlc.read.parquet(Parquet_Trips_File).write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"trip",DBConnectionProperties)
//    sqlc.read.parquet(Parquet_Stops_File).write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"stop",DBConnectionProperties)
//    sqlc.read.parquet(Parquet_Shapes_File).write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"shape",DBConnectionProperties)
//    sqlc.read.parquet(Parquet_TripStops_File).write.mode(SaveMode.Overwrite).jdbc(DBConnectionString,"tripstop",DBConnectionProperties)
//  }


}
