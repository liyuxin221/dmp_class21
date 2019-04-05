package cn.itcast.dmp.etl

import cn.itcast.dmp.`trait`.ProcessData
import cn.itcast.dmp.tools._
import org.apache.kudu.Schema
import org.apache.spark.sql.{DataFrame, SparkSession}

//广告投放的联网方式分布情况统计
object AdNetWorkAnalysis  extends ProcessData{

  private val kuduMaster: String = GlobalConfigUtils.kuduMaster
  private val tableName: String = GlobalConfigUtils.odsPrefix + DateUtils.getNowDay

  val sinkTable="adNetWorkAnalysis"

  val kuduOptions=Map(
    "kudu.master" -> kuduMaster,
    "kudu.table" ->tableName
  )

  /**
    * 处理数据，实现不同的etl操作
    *
    * @param sparkSession
    */
  override def process(sparkSession: SparkSession): Unit = {
    import org.apache.kudu.spark.kudu._

    //1、加载ods表的数据
    val odsDF: DataFrame = sparkSession.read.options(kuduOptions).kudu

    //2、分析处理--广告投放的手机设备类型分布情况统计
    odsDF.createOrReplaceTempView("ods")
    val adNetWorkDF: DataFrame = sparkSession.sql(ContantsSQL.adNetWorkSQL1)

    adNetWorkDF.createOrReplaceTempView("adNetWork")
    val result: DataFrame = sparkSession.sql(ContantsSQL.adNetWorkSQL2)

    //3、保存结果数据到kudu表中
    val schema: Schema = ContantsSchema.adNetWorkSchema
    val partitionID="networkmannerid"
    DBUtils.saveData2Kudu(result,kuduMaster,sinkTable,schema,partitionID)


  }

}
