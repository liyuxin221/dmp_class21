import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  */
object TestGraphx {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("TestGraphx").setMaster("local[2]")


    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //spark的图
    //图---->点,边


    //构建点集合
    val vertexRDD: RDD[(VertexId, (String, Int))] = sc.parallelize(List(
      (1, ("张三", 18)),
      (2, ("李四", 19)),
      (3, ("王五", 20)),
      (4, ("赵六", 21)),
      (5, ("韩梅梅", 22)),
      (6, ("李雷", 23)),
      (7, ("小明", 24)),
      (9, ("tom", 25)),
      (10, ("jerry", 26)),
      (11, ("ession", 27))
    ))

    //构建边集合
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(List(
      Edge(1, 136, 0),
      Edge(2, 136, 0),
      Edge(3, 136, 0),
      Edge(4, 136, 0),
      Edge(5, 136, 0),
      Edge(4, 158, 0),
      Edge(5, 158, 0),
      Edge(6, 158, 0),
      Edge(7, 158, 0),
      Edge(9, 177, 0),
      Edge(10, 177, 0),
      Edge(11, 177, 0)
    ))

    //构件图
    val graph = Graph(vertexRDD,edgeRDD)

    println("打印图中的所有的点.")
    graph.vertices.foreach(println)

    println("打印图中的所有的边.")
    graph.edges.foreach(println)

    println("---------------------")
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    val edges: EdgeRDD[Int] = graph.connectedComponents().edges

    //(对象id,聚合点id) join (对象id,(name,age))---->(对象id,(聚合点id,(name,age)))
    val joinRDD: RDD[(VertexId, (VertexId, (String, PartitionID)))] = vertices.join(vertexRDD)

    joinRDD.foreach(println)

    println("---------------------")

    val aggrRDD: RDD[(VertexId, List[(VertexId, String, PartitionID)])] = joinRDD.map {
      case (userid, (aggrid, (name, age))) => (aggrid, List((userid, name, age)))
    }
    aggrRDD.foreach(println)

    println("---------------------")

    val result: RDD[(VertexId, List[(VertexId, String, PartitionID)])] = aggrRDD.reduceByKey(_++_)

    result.foreach(println)


    sc.stop()
  }
}
