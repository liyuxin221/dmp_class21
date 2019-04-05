package cn.itcast.dmp.etl

import cn.itcast.dmp.`trait`.ProcessData
import cn.itcast.dmp.tools._
import org.apache.kudu.Schema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.kudu.spark.kudu._

//统计各省市的地域分布情况
object ProcessRegion extends ProcessData{

   //定义kudumaster的地址
  private val kuduMaster: String = GlobalConfigUtils.kuduMaster

  //定义ods表名
  private val sourceTable: String = GlobalConfigUtils.odsPrefix + DateUtils.getNowDay

  //定义数据保存的表名--->正常也需要每天生成一张表，这里就不这样处理
  val sinkTable="processRegion"

  //定义map集合
  val kuduOptions=Map(
    "kudu.master" -> kuduMaster,
    "kudu.table" -> sourceTable
  )

  /**
    * 处理数据，实现不同的etl操作
    *
    * @param sparkSession
    */
  override def process(sparkSession: SparkSession): Unit = {

     //1、加载ods表的数据
     val odsDF: DataFrame = sparkSession.read.options(kuduOptions).kudu

    //2、分析处理-统计各省市的地域分布情况
     odsDF.createOrReplaceTempView("ods")
     val result: DataFrame = sparkSession.sql(ContantsSQL.regionsql)

    //3、保存结果数据到kudu
    val schema: Schema = ContantsSchema.processRegionSchema
    //分区字段
    val partitionID="provincename"

    DBUtils.saveData2Kudu(result,kuduMaster,sinkTable,schema,partitionID )

  }
}
