import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, Normalizer, StandardScaler, VectorAssembler}
import org.apache.spark.sql.functions.{avg, col, map, round}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

val spark: SparkSession =  SparkSession.builder()
  .appName("ex4")
  .config("spark.driver.host", "localhost")
  .master("local")
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

val df: DataFrame = spark.read.options(Map("inferSchema"->"true", "header"->"true")).csv("Documents/DIP/Projects/scala/data/dataD2.csv")
df.show()
//
//def task_kmeans(df: DataFrame, k: Int, input_cols: Array[String], select_expr_1: Array[String], agg_map: Map[String, String], select_expr_2: Array[String]) = {
//
//  val scaler = new Normalizer().setInputCol("features_raw").setOutputCol("features")
//  val vectorAssembler = new VectorAssembler().setInputCols(input_cols).setOutputCol("features_raw")
//  val kmeans = new KMeans().setK(k)
//
//  val data_pipline = new Pipeline().setStages(Array(
//    vectorAssembler, scaler, kmeans
//  ))
//
//  val kModel = data_pipline.fit(df)
//
//  kModel.transform(df).withColumn("features",
//    vector_to_array(col("features"))
//  )
//    .selectExpr(select_expr_1:_*)
//    .groupBy("prediction")
//    .agg(agg_map)
//    .selectExpr(select_expr_2:_*)
//    .collect()
//}
//
//task_kmeans(df, 5,
//  Array("a", "b", "c"),
//  Array("features[0] as a_scaled", "features[1] as b_scaled", "features[2] as c_scaled", "a", "b", "c", "prediction"),
//  Map("a_scaled"->"mean", "b_scaled"->"mean", "c_scaled"->"mean", "a"->"mean", "b"->"mean", "c"->"mean"),
//  Array("round(`avg(a_scaled)`, 3)", "round(`avg(b_scaled)`, 3)", "round(`avg(c_scaled)`, 3)")
//).map(r => (r.getDouble(0), r.getDouble(1), r.getDouble(2)))


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



def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {
  def task4_recursion(l: Int, h: Int, res: Array[(Int, Double)]): Array[(Int, Double)] = {
    if (l == h ) {
      res ++ Array((h, task_kmeans_score(df, h, Array("a", "b"))))
    } else {
      task4_recursion(l, h-1, res ++ Array((h, task_kmeans_score(df, h, Array("a", "b")))))
    }
  }

  task4_recursion(low, high, Array())
}

task4(df, 3, 6)
spark.close()