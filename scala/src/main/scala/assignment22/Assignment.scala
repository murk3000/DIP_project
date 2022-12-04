package assignment22

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{MinMaxScaler, Normalizer, StandardScaler, VectorAssembler}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, round, when}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class Assignment {

  val spark: SparkSession =  SparkSession.builder()
    .appName("ex4")
    .config("spark.driver.host", "localhost")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")   // suppress informational or warning log messages related to the inner working of Spark
  val dataD2_schema = StructType(Array(
    StructField("a",DoubleType,true),
    StructField("b",DoubleType,true),
    StructField("LABEL",StringType,true)
  ))

  val dataD3_schema = StructType(Array(
    StructField("a",DoubleType,true),
    StructField("b",DoubleType,true),
    StructField("c",DoubleType,true),
    StructField("LABEL",StringType,true)
  ))

  // the data frame to be used in tasks 1 and 4
  val dataD2: DataFrame = spark.read.options(Map("header"->"true")).schema(dataD2_schema).csv("data/dataD2.csv").na.drop

////  To use dirty data we can use the following piece of code
//  val dataD2_dirty: DataFrame = spark.read.options(Map("header"->"true")).schema(dataD2_schema).csv("data/dataD2_dirty.csv")
//  val dataD2 = dataD2_dirty.na.drop



  // the data frame to be used in task 2
  val dataD3: DataFrame = spark.read.options(Map("header"->"true")).schema(dataD3_schema).csv("data/dataD3.csv")

  // the data frame to be used in task 3 (based on dataD2 but containing numeric labels)
  val dataD2WithLabels: DataFrame = dataD2.withColumn("label_numeric", when(col("LABEL")==="Fatal", 1).otherwise(0))

  def task_kmeans(df: DataFrame, k: Int, input_cols: Array[String], select_expr_1: Array[String], agg_map: Map[String, String], select_expr_2: Array[String]) = {

    val scaler = new MinMaxScaler().setInputCol("features_raw").setOutputCol("features")
    val vectorAssembler = new VectorAssembler().setInputCols(input_cols).setOutputCol("features_raw")
    val kmeans = new KMeans().setK(k)

    val data_pipline = new Pipeline().setStages(Array(
      vectorAssembler, scaler, kmeans
    ))

    val kModel = data_pipline.fit(df)

    kModel.transform(df).withColumn("features",
      vector_to_array(col("features"))
    )
      .selectExpr(select_expr_1:_*)
      .groupBy("prediction")
      .agg(agg_map)
      .selectExpr(select_expr_2:_*)
  }

  def task_kmeans_score(df: DataFrame, k: Int, input_cols: Array[String]) = {

    val scaler = new MinMaxScaler().setInputCol("features_raw").setOutputCol("features")
    val vectorAssembler = new VectorAssembler().setInputCols(input_cols).setOutputCol("features_raw")
    val kmeans = new KMeans().setK(k)

    val data_pipline = new Pipeline().setStages(Array(
      vectorAssembler, scaler, kmeans
    ))

    val kModel = data_pipline.fit(df)
    val evaluator = new ClusteringEvaluator()

    evaluator.evaluate(kModel.transform(df))
  }
//// function hardcoded with number of column
//  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {
//
//    val vectorAssembler = new VectorAssembler().setInputCols(Array("a", "b")).setOutputCol("features_raw")
//    val scaler = new MinMaxScaler().setInputCol("features_raw").setOutputCol("features")
//    val kmeans = new KMeans().setK(k)
//
//    val data_pipline = new Pipeline().setStages(Array(
//      vectorAssembler, scaler, kmeans
//    ))
//
//    val kModel = data_pipline.fit(df)
//
//    kModel.transform(df).withColumn("features",
//      vector_to_array(col("features"))
//    )
//      .selectExpr("a", "b", "prediction")
//      .groupBy("prediction")
//      .agg(Map("a"->"mean", "b"->"mean"))
//      .selectExpr(Array("round(`avg(a)`, 3)", "round(`avg(b)`, 3)") :_*)
//      .collect()
//      .map(r=>(r.getDouble(0), r.getDouble(1)))
//  }

  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {
//    task_kmeans(df, k,
//      Array("a", "b"),                                                              // InputCols
//      Array("features[0] as a_scaled", "features[1] as b_scaled", "prediction"),    // The columns needed to calculate cluster centers
//      Map("a_scaled"->"mean", "b_scaled"->"mean"),                                  // aggregations needed
//      Array("round(`avg(a_scaled)`, 3)", "round(`avg(b_scaled)`, 3)")               // What columns to return
//    ).collect().map(r => (r.getDouble(0), r.getDouble(1)))

    // This returns the original a,b values for the cluster centers
    task_kmeans(df, k,
      Array("a", "b"),                                      // InputCols
      Array("a", "b", "prediction"),                        // The columns needed to calculate cluster centers
      Map("a"->"mean", "b"->"mean"),                        // aggregations needed
      Array("round(`avg(a)`, 3)", "round(`avg(b)`, 3)")     // What columns to return
    ).collect().map(r => (r.getDouble(0), r.getDouble(1)))
  }

  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {
    task_kmeans(df, k,
      Array("a", "b", "c"),
      Array("features[0] as a_scaled", "features[1] as b_scaled", "features[2] as c_scaled", "prediction"),
      Map("a_scaled"->"mean", "b_scaled"->"mean", "c_scaled"->"mean"),
      Array("round(`avg(a_scaled)`, 3)", "round(`avg(b_scaled)`, 3)", "round(`avg(c_scaled)`, 3)")
    ).collect().map(r => (r.getDouble(0), r.getDouble(1), r.getDouble(2)))

//    // This returns the original a,b,c values for the cluster centers
//    task_kmeans(df, k,
//      Array("a", "b", "c"),
//      Array("a", "b", "c", "prediction"),
//      Map("a"->"mean", "b"->"mean", "c"->"mean"),
//      Array("round(`avg(a)`, 3)", "round(`avg(b)`, 3)", "round(`avg(c)`, 3)")
//    ).collect().map(r => (r.getDouble(0), r.getDouble(1), r.getDouble(2)))
  }

  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {
    task_kmeans(df, k,
      Array("a", "b", "label_numeric"),
      Array("features[0] as a_scaled", "features[1] as b_scaled", "label_numeric", "prediction"),
      Map("a_scaled"->"mean", "b_scaled"->"mean", "label_numeric"->"sum"),
      Array("round(`avg(a_scaled)`, 3)", "round(`avg(b_scaled)`, 3)", "`sum(label_numeric)`")
    ).sort(col("sum(label_numeric)").desc)
      .limit(2)
      .collect()
      .map(r => (r.getDouble(0), r.getDouble(1)))
  }

  // Parameter low is the lowest k and high is the highest one.
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {
    def task4_recursion(l: Int, h: Int, res: Array[(Int, Double)]): Array[(Int, Double)] = {
      if (l >= h ) {
        res ++ Array((h, task_kmeans_score(df, h, Array("a", "b"))))
      } else {
        task4_recursion(l, h-1, res ++ Array((h, task_kmeans_score(df, h, Array("a", "b")))))
      }
    }

    val scores = task4_recursion(low, high, Array())



    scores
  }

}
