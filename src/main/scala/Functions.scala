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
import org.apache.spark.sql.{DataFrame, SaveMode, functions}
import org.apache.spark.sql.functions._

import scala.collection.mutable._


// Functions to operate and manipulate GPS data.
// Global variables and minor IO/formatting functions
// can be found at Global.scala

object Functions {

  //  def Convert() {
  //    GetRoutesDF().write.format("parquet").mode(SaveMode.Overwrite).save(HDFS_Dir + "gtfs/routes.parquet")
  //    GetFaresDF().write.format("parquet").mode(SaveMode.Overwrite).save(HDFS_Dir + "gtfs/fares.parquet")
  //    GetTripsDF().write.format("parquet").mode(SaveMode.Overwrite).save(HDFS_Dir + "gtfs/trips.parquet")
  //    GetStopsDF().write.format("parquet").mode(SaveMode.Overwrite).save(HDFS_Dir + "gtfs/stops.parquet")
  //    GetShapesDF().write.format("parquet").mode(SaveMode.Overwrite).save(HDFS_Dir + "gtfs/shapes.parquet")
  //  }

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
    //    Convert()
    UpdateTripStops()
  }

  //    def Convert() {
  //      val a = SparkSqlContext.read.jdbc(DBConnectionString,"tripstop_",DBConnectionProperties)
  //      a.write.mode(SaveMode.Overwrite).parquet(Parquet_TripStops_File)
  //    }

  def GetRoutesDF(route_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading routes table... ")

    var data = SparkSqlContext.read.parquet(Parquet_Routes_File)

    if (data.count() == 0) {
      println("No data")
      UpdateFaresAndRoutes()
      return GetRoutesDF(route_id, true)

    }

    if (route_id != null)
      data = data.filter(col("id") === route_id)

    //    val data_arr = data.collect().map(f =>
    //      Route(id = f(0).toString, code = f(1).toString, name = f(2).toString,
    //        info = f(3).toString, color = f(4).toString, fare_id = f(5).toString))
    //
    //    val data_rdd = SparkGlobalContext.broadcast(data_arr)
    //    val data_df = SparkSqlContext.createDataFrame(data_rdd.value)

    if (debug && !recursive) println("Loaded. Count: " + data.count())

    data
  }

  def GetFaresDF(fare_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading fares table... ")

    var data = SparkSqlContext.read.parquet(Parquet_Fares_File)

    if (data.count() == 0) {
      println("No data")
      UpdateFaresAndRoutes()
      return GetFaresDF(fare_id, true)

    }

    if (fare_id == null)
      data = data.filter(col("id") === fare_id)

    //    val data_arr = data.collect().map(f =>
    //      Fare(id = f(0).toString, price = f(1).toString.toFloat,
    //        transfers = f(2).toString.toInt, transfer_duration = f(3).toString.toInt))
    //
    //    val data_rdd = SparkGlobalContext.broadcast(data_arr)
    //    val data_df = SparkSqlContext.createDataFrame(data_rdd.value)

    if (debug && !recursive) println("Loaded. Count: " + data.count())

    data
  }

  def GetShapesDF(shape_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading shapes table... ")

    var data = SparkSqlContext.read.parquet(Parquet_Shapes_File)

    if (data.count() == 0) {
      println("No data")
      UpdateShapes()
      return GetShapesDF(shape_id, true)

    }

    if (shape_id != null)
      data = data.filter(col("id") === shape_id)

    //val data_brd = SparkGlobalContext.parallelize(data)
    if (debug && !recursive) println("Loaded. Count: " + data.count() + "\r\n" +
      "ATTENTION: Need to call some ACTION to avoid lazy-transformation problems")

    broadcast(data)
  }

  def GetStopsDF(stop_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading stops table... ")

    var data = SparkSqlContext.read.parquet(Parquet_Stops_File)

    if (data.count() == 0) {
      println("No data")
      UpdateStops()
      return GetStopsDF(stop_id, true)
    }

    if (stop_id != null)
      data = data.filter(col("id") === stop_id)

    //    val data_brd = SparkGlobalContext.broadcast(data)

    if (debug && !recursive) println("Loaded. Count: " + data.count())

    data
  }

  def GetTripsDF(trip_id: String = null, route_id: String = null, recursive: Boolean = false): DataFrame = {

    if (debug && !recursive) print("Reading trips table... ")

    var data: DataFrame = SparkSqlContext.read.parquet(Parquet_Trips_File)

    if (data.count() == 0) {
      println("No data")
      UpdateTrips()
      return GetTripsDF(trip_id, route_id, true)
    }

    if (trip_id != null && route_id != null)
      data = data.filter(col("id") === trip_id && col("route_id") === route_id)
    else if (trip_id == null && route_id != null)
      data = data.filter(col("route_id") === route_id)
    else if (trip_id != null && route_id == null)
      data = data.filter(col("id") === trip_id)

    //    val data_brd = SparkGlobalContext.broadcast(data)

    if (debug && !recursive) println("Loaded. Count: " + data.count())

    data
  }

  def UpdateFaresAndRoutes() {

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

    fare_attribs_df.write.format("parquet").mode(SaveMode.Overwrite).save(Parquet_Fares_File)
    routes_df.write.format("parquet").mode(SaveMode.Overwrite).save(Parquet_Routes_File)

    fare_attribs_df.unpersist()
    fare_rules_df.unpersist()
    routes_df.unpersist()
    routes_csv.unpersist()
  }

  def UpdateShapes() {

    // Erasing existent file

    val ss = new ArrayBuffer[Shape]() += new Shape("", 0, .0, .0, .0, .0, .0, .0)
    val r = SparkGlobalContext.parallelize(ss)
    SparkSqlContext.createDataFrame(r).write.format("parquet").mode(SaveMode.Overwrite).save(Parquet_Shapes_File)

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
    var shapes_count = 0

    print("ids: ")
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

      val shapes_rdd = SparkGlobalContext.parallelize(shapes_calculated)
      val shapes_df = SparkSqlContext.createDataFrame(shapes_rdd)
      shapes_df.write.format("parquet").mode(SaveMode.Append).save(Parquet_Shapes_File)
      shapes_calculated = new ArrayBuffer[Shape]()
      //        ShapesDF = shapes_df
      //        ShapesDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      //      shapes_df.unpersist()
      //      shapes_rdd.unpersist()

      if (debug) print(id + " | ")
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

    stops_df.write.format("parquet").mode(SaveMode.Overwrite).save(Parquet_Stops_File)

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

    trips_df.write.format("parquet").mode(SaveMode.Overwrite).save(Parquet_Trips_File)

  }

  def UpdateTripStops() {

    // Erasing existent file

    val ss = new ArrayBuffer[TripStop]() += new TripStop(0, null, 0, .0)
    val r = SparkGlobalContext.parallelize(ss)
    SparkSqlContext.createDataFrame(r).write.format("parquet").mode(SaveMode.Overwrite).save(Parquet_TripStops_File)

    // --------------------------------------------- stop_times.txt

    val trips_df = GetTripsDF().select("id", "shape_id").withColumnRenamed("id", "trip_id")
    broadcast(trips_df)

    val stops_df = GetStopsDF()
    broadcast(stops_df)

    val shapes_df = GetShapesDF().drop("sequence")
    broadcast(shapes_df)

      if (debug) println("Reading stop_times.txt")

      val stop_times_csv = SparkGlobalContext.textFile(GTFS_Stop_Times_File)
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

    val stop_times_df = SparkSqlContext.createDataFrame(stop_times_txt_rdd)
      .toDF("trip_id", "stop_id", "sequence").join(trips_df, "trip_id")
    println("Filtered. Count: " + stop_times_df.count())
    //      stop_times_df.printSchema() // trip_id, stop_id, sequence(stop), shape_id
      // 21590
    stop_times_df.write.format("parquet").mode(SaveMode.Overwrite).save(Parquet_StopTimes_File)

      stop_times_txt_rdd.unpersist()

      if (debug) println("Calculating stop distances by trip_id. Loop: ")

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

      if (debug) print("| Steps OK: ")
        // Filtering and collecting relationship trip<=>stop by trip_id
        val df1 = stop_times_df //.filter(col("trip_id") === trip_id).drop("trip_id")
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
        val df3a = df2 //SparkSqlContext.read.parquet(HDFS_Dir + Parquet_Data + "temp.parquet")
      //          .withColumn("onSegment", functions.callUDF("onSegment", col("lat"), col("lon"), col("lat1"), col("lon1"), col("lat2"), col("lon2")))
      df3a.registerTempTable("df3a")
      val df3 = SparkSqlContext.sql("select * from df3a where onSegment(lat,lon,lat1,lon1,lat2,lon2) = true")
      //        df3.write.format("parquet").mode(SaveMode.Overwrite).save(Local_Dir + GTFS_Data + "df3.parquet")
      //        df3.unpersist()
      if (debug) print("3 ")

      val df4 = df3 //SparkSqlContext.read.parquet(Local_Dir + GTFS_Data + "df3.parquet")
        //          .filter(col("onSegment") === lit(true))
        .withColumn("alongTrackDistance",
        functions.callUDF("alongTrack", df3("lat"), df3("lon"), df3("lat1"), df3("lon1"), df3("lat2"), df3("lon2")))
      //        df4.write.format("parquet").mode(SaveMode.Overwrite).save(Local_Dir + GTFS_Data + "df4.parquet")
      //        df4.unpersist()
      if (debug) print("4 ")

      val df5 = df4 //SparkSqlContext.read.parquet(Local_Dir + GTFS_Data + "df4.parquet")
        .withColumn("stop_dist", df4("total_dist") - df4("dist") + df4("alongTrackDistance"))
      //        df5.write.format("parquet").mode(SaveMode.Overwrite).save(Local_Dir + GTFS_Data + "df5.parquet")
      //        df5.unpersist()
      if (debug) print("5 ")

      val df6 = df5 //SparkSqlContext.read.parquet(Local_Dir + GTFS_Data + "df5.parquet")
          .drop("dist")
        .withColumnRenamed("stop_dist", "dist")
        //.withColumn("trip_id", lit(trip_id))
        .select("trip_id", "stop_id", "sequence", "dist")
      if (debug) print("6 ")

    //      df6.write.mode(SaveMode.Overwrite).parquet(Parquet_TripStops_File)
    df6.write.mode(SaveMode.Overwrite).jdbc(DBConnectionString, "tripstop_", DBConnectionProperties)
      if (debug) println("DB.")

      df3a.unpersist()
      df3.unpersist()
      df4.unpersist()
      df5.unpersist()
      df6.unpersist()

      //        if (debug) println("-----")
    //    }
    //      }

    //remove(HDFS_Dir + Parquet_Data + "temp")

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

