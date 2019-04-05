package cn.itcast.dmp.trade

import java.util

import ch.hsr.geohash.GeoHash
import cn.itcast.dmp.`trait`.ProcessData
import cn.itcast.dmp.bean.{BTrade, BusinessArea}
import cn.itcast.dmp.tools._
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.lang.StringUtils
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//生成商圈库
object TradeAnalysis extends ProcessData {

  //获取kudumaster地址
  private val kuduMaster: String = GlobalConfigUtils.kuduMaster

  //获取ods层表名     ODS+日期----->ODS20190401
  private val sourceTable: String = GlobalConfigUtils.odsPrefix + DateUtils.getNowDay

  //定义map集合
  val kuduOptions = Map(
    "kudu.master" -> kuduMaster,
    "kudu.table" -> sourceTable
  )

  //定义结果数据保存的表名
  val sinkTable="trade"


  /**
    * 处理数据,实现不同的ETL操作
    *
    * @param sparkSession
    */
  override def process(sparkSession: SparkSession): Unit = {

    //1.获取ods层的数据
    val odsDF: DataFrame = sparkSession.read.options(kuduOptions).kudu

    //2.获取商圈信息
    odsDF.createOrReplaceTempView("ods")
    //对ods表进行过滤,把国内的经纬度地址过滤出来  经度:73~136,维度 3~54
    val chinaDF: DataFrame = sparkSession.sql(ContantsSQL.filter_non_china_sql)
    val rowRDD: RDD[Row] = chinaDF.rdd

    val bTradeRDD: RDD[BTrade] = rowRDD.map(row => {
      //获取经度
      val longitude: String = row.getAs[String]("long")
      //获取维度
      val latitude: String = row.getAs[String]("lat")

      val location = longitude + "," + latitude
      //高德API https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=d963875ab3edf4c53cf8dc058c1f39e5&radius=1000&extensions=all

      val trades: String = getTrades(location)

      //使用geohash算法,对经纬度处理之后生成一个唯一的标识
      val geoHashCode: String = GeoHash.withCharacterPrecision(latitude.toDouble, longitude.toDouble, 8).toBase32

      //返回值
      BTrade(geoHashCode, trades)
    }).filter(x => !"".equals(x.trades))

    import sparkSession.implicits._
    val result: DataFrame = bTradeRDD.toDF

    //3.保存结果数据
    val schema: Schema = ContantsSchema.tradeSchema
    val partitionID="geoHashCode"
    DBUtils.saveData2Kudu(result,kuduMaster,sinkTable,schema,partitionID)

  }


  //获取经纬度对应的商圈信息
  def getTrades(location: String): String = {
    //高德API https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=d963875ab3edf4c53cf8dc058c1f39e5

    var url = "https://restapi.amap.com/v3/geocode/regeo?location=" + location + "&key=" + GlobalConfigUtils.getKey

    val httpClient = new HttpClient()
    val getMethod = new GetMethod(url)

    val status: Int = httpClient.executeMethod(getMethod)

    var flag = ""
    if (status == 200) {
      val response = getMethod.getResponseBodyAsString

      flag = parseResponse(response)
    }
    flag

  }

  //解析response信息获取商圈信息
  def parseResponse(response: String): String = {
    val jSONObject: JSONObject = JSON.parseObject(response)
    val regeocode: JSONObject = jSONObject.get("regeocode").asInstanceOf[JSONObject]
    val addressComponent: JSONObject = regeocode.get("addressComponent").asInstanceOf[JSONObject]
    val businessAreas: JSONArray = addressComponent.getJSONArray("businessAreas")

    val businessAreasList: util.List[BusinessArea] = JSON.parseArray(businessAreas.toString,classOf[BusinessArea])

    import scala.collection.JavaConversions._
    val toList: List[BusinessArea] = businessAreasList.toList

    var flag = ""
    //遍历
    if (toList != null && toList.length > 0) {
      val sb = new StringBuffer()
      for (t <- toList) {
        if (t != null) {
          sb.append(t.name + ":")
        }
      }

      val value: String = sb.toString
      if (StringUtils.isNotBlank(value)) {
        flag = value.substring(0, value.length - 1)
      }
    }
    flag
  }

}
