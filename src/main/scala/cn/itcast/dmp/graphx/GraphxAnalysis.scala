package cn.itcast.dmp.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

//使用spark进行图计算
object GraphxAnalysis {

  //                      userid ,用户的所有标识         用户的所有标签
  def graphx(tagsRDD: RDD[(String, (List[(String, Int)], List[(String, Double)]))]) = {
    //构建点集合
    val vertexRDD: RDD[(Long, (List[(String, Int)], List[(String, Double)]))] = tagsRDD.map {
      case (userid, (useridsList, userTagsList)) => (userid.hashCode.toLong, (useridsList, userTagsList))
    }

    //构建边集合   //userid  用户的每一个标识
    val valuesRDD: RDD[(String, (String, Int))] = tagsRDD.flatMapValues(x=>x._1)
    val edgeRDD: RDD[Edge[Int]] = valuesRDD.map(x=>Edge(x._1.hashCode.toLong,x._2._1.hashCode.toLong,0))

    //构件图
    val graph: Graph[(List[(String, Int)], List[(String, Double)]), Int] = Graph(vertexRDD,edgeRDD)

    //构建连通图         (userid,aggrid)
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    // (userid,aggrid) join (userid,(用户的所有标识,用户的所有的标签))---->(userid,(aggrid,((用户的所有标识,用户的所有的标签))))
    val joinRDD: RDD[(VertexId, (VertexId, (List[(String, Int)], List[(String, Double)])))] = vertices.join(vertexRDD)

    val result: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = joinRDD.map {
      case (userid, (aggrid, (useridsList, userTagsList))) => (aggrid, (useridsList, userTagsList))
    }

    result
  }
}
