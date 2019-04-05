package cn.itcast.dmp.tags

import java.util

import cn.itcast.dmp.`trait`.ProcessData
import cn.itcast.dmp.graphx.GraphxAnalysis
import cn.itcast.dmp.tools.{ContantsSQL, DateUtils, GlobalConfigUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


//todo:给用户打标签
object DataTags extends ProcessData {
  private val kuduMaster: String = GlobalConfigUtils.kuduMaster
  private val tableName: String = GlobalConfigUtils.odsPrefix + DateUtils.getNowDay

  val sinkTable = "adAppAnalysis"

  val kuduOptions = Map(
    "kudu.master" -> kuduMaster,
    "kudu.table" -> tableName
  )


  /**
    * 处理数据，实现不同的etl操作
    *
    * @param sparkSession
    */
  override def process(sparkSession: SparkSession): Unit = {
    /**
      * 1、把一些用户标识的字段信息过滤掉
      * 2、给用户打标签：性别、年龄、地域、广告位类型、设备类型(手机操作系统、联网方式、运营商）、app名称、关键字、商圈、用户的标识等
      */

    //1、加载ods表数据
    import org.apache.kudu.spark.kudu._
    val odsDF: DataFrame = sparkSession.read.options(kuduOptions).kudu
    //把一些丢失用户标识的信息去掉
    odsDF.createOrReplaceTempView("ods")
    val rightDF: DataFrame = sparkSession.sql(ContantsSQL.filter_non_empty_sql)
    val rowRDD: RDD[Row] = rightDF.rdd


    //2、加载数据字典文件
    //2.1 加载appid-name文件               XRX100003##YY直播
    val sc: SparkContext = sparkSession.sparkContext
    val appIDRDD: RDD[(String, String)] = sc.textFile(GlobalConfigUtils.appIDName).map(x => x.split("##")).map(x => (x(0), x(1)))
    val appIDArray: Array[(String, String)] = appIDRDD.collect()
    val appMap: Map[String, String] = appIDArray.toMap
    val appMapBroadcast: Broadcast[Map[String, String]] = sc.broadcast(appMap)


    //2.2 加载devicedic文件              WIFI##D00020001
    val devicedicRDD: RDD[(String, String)] = sc.textFile(GlobalConfigUtils.devicedic).map(x => x.split("##")).map(x => (x(0), x(1)))
    val devicedicArray: Array[(String, String)] = devicedicRDD.collect()
    val devicedicMap: Map[String, String] = devicedicArray.toMap
    val devicedicMapBroadcast: Broadcast[Map[String, String]] = sc.broadcast(devicedicMap)


    //打标签的处理逻辑   //(userid,(用户的所有标识,用户的所有标签数据))
    val tagsRDD: RDD[(String, (List[(String, Int)], List[(String, Double)]))] = rowRDD.map(row => {
      //todo:1、性别标签
      val sexMap: Map[String, Double] = SexTags.makeTags(row)

      //todo: 2、年龄标签
      val ageMap: Map[String, Double] = AgeTags.makeTags(row)

      //todo: 3、地域标签
      val regionMap: Map[String, Double] = RegionTags.makeTags(row)

      //todo: 4、广告位类型标签
      val adSpaceTypeMap: Map[String, Double] = AdSpaceTypeTags.makeTags(row)

      //todo: 5、设备类型标签
      val adDeviceTypeMap: Map[String, Double] = AdDeviceTags.makeTags(row, devicedicMapBroadcast.value)

      //todo: 6、app名称标签
      val appMap: Map[String, Double] = AppTags.makeTags(row, appMapBroadcast.value)

      //todo: 7、关键字标签
      val keywordsMap: Map[String, Double] = KeyWordsTags.makeTags(row)

      //todo: 8、商圈标签
      val tradeMap: Map[String, Double] = TradeTags.makeTags(row)

      //todo: 用户的标识标签
      val useridsList: util.LinkedList[String] = UserTags.makeTags(row)
      //将list中第一个不为空的元素看成为userid
      val userid: String = useridsList.getFirst

      import scala.collection.JavaConversions._
      val userAllList: List[(String, Int)] = useridsList.toList.map(x => (x, 0))


      val tags: Map[String, Double] = sexMap ++ ageMap ++ ageMap ++ regionMap ++ adSpaceTypeMap ++ adDeviceTypeMap ++ appMap ++ keywordsMap ++ tradeMap

      //(用户的id,(userAllList,tags))
      (userid, (userAllList, tags.toList))
    })

    //    tagsRDD.foreach(x=>println(x))

    //    tagsRDD.repartition(1).saveAsTextFile("src/main/resources/tags0406")

    //使用spark图计算,来识别多条数据是同一个用户产生的----用户的统一识别
    val graphxRDD: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = GraphxAnalysis.graphx(tagsRDD)

    graphxRDD

  }
}
