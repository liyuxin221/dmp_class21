package cn.itcast.dmp.etl

import cn.itcast.dmp.`trait`.ProcessData
import cn.itcast.dmp.bean.IpRegion
import cn.itcast.dmp.tools._
import cn.itcast.dmp.tools.iplocation.{IPAddressUtils, IPLocation}
import com.maxmind.geoip.{Location, LookupService}
import org.apache.kudu.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//todo: 根据ip地址解析经纬度省市
object ImproveData  extends ProcessData{
  //获取kudumaster地址
   private val kuduMaster: String = GlobalConfigUtils.kuduMaster

  //获取ods层表名     ODS+日期----->ODS20190401
  private val tableName: String = GlobalConfigUtils.odsPrefix + DateUtils.getNowDay



  /**
    * 处理数据，实现不同的etl操作
    *
    * @param sparkSession
    */
  override def process(sparkSession: SparkSession): Unit = {
    //1、加载原始数据源
     val odsDF: DataFrame = sparkSession.read.format("json").load(GlobalConfigUtils.getDataPath)

    //2、处理分析--根据ip地址解析经纬度省市
    val ipsDF: DataFrame = odsDF.select("ip")
    val rowRDD: RDD[Row] = ipsDF.rdd

    val ipRegionRDD: RDD[IpRegion] = rowRDD.map(row => {
      //获取ip地址
      val ip: String = row.getAs[String]("ip")

      //根据ip地址解析得到经纬度、省市
      val lookupService = new LookupService(GlobalConfigUtils.geoLiteCityDat)
      val location: Location = lookupService.getLocation(ip)
      //经度
      val longitude: Float = location.longitude
      //维度
      val latitude: Float = location.latitude

      //根据ip获取省市
      val addressUtils = new IPAddressUtils
      val iPLocation: IPLocation = addressUtils.getregion(ip)
      //省份
      val province: String = iPLocation.getRegion
      //市
      val city: String = iPLocation.getCity

      IpRegion(ip, longitude.toString, latitude.toString, province, city)
    })

    import sparkSession.implicits._
    val ipRegionDF: DataFrame = ipRegionRDD.toDF

    //把odsDF和ipRegionDF分别注册表
    odsDF.createOrReplaceTempView("ods")
    ipRegionDF.createOrReplaceTempView("region")

    val result: DataFrame = sparkSession.sql(ContantsSQL.odssql)

    //3、保存到kudu表中
    val schema: Schema = ContantsSchema.odsSchema

    //分区字段
    val partitionID="ip"

    DBUtils.saveData2Kudu(result,kuduMaster,tableName ,schema,partitionID)

  }
}
