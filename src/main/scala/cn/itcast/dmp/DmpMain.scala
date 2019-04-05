package cn.itcast.dmp

import cn.itcast.dmp.etl._
import cn.itcast.dmp.tags.DataTags
import cn.itcast.dmp.tools.GlobalConfigUtils
import cn.itcast.dmp.trade.TradeAnalysis
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

//todo:项目的执行入口，后期的大量处理逻辑都在这个object
object DmpMain {
  def main(args: Array[String]): Unit = {

    /**
      *spark.worker.timeout="500"
      *spark.rpc.askTimeout="600s"
      *spark.network.timeout="600s"
      *spark.cores.max="10"
      *spark.task.maxFailures="5"
      *spark.speculation="true"
      *spark.driver.allowMutilpleContext="true"
      *spark.serializer="org.apache.spark.serializer.KryoSerializer"
      *spark.buffer.pageSize="8m"
      */
    //1、构建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("DmpMain")
      .setMaster("local[3]")
      .set("spark.worker.timeout", GlobalConfigUtils.sparkWorkerTimeout)
      .set("spark.worker.timeout", GlobalConfigUtils.sparkWorkerTimeout)
      .set("spark.rpc.askTimeout", GlobalConfigUtils.sparkRpcAskTimeout)
      .set("spark.network.timeout", GlobalConfigUtils.sparkNetworkTimeout)
      .set("spark.cores.max", GlobalConfigUtils.sparkCoresMax)
      .set("spark.task.maxFailures", GlobalConfigUtils.sparkTaskMaxFailures)
      .set("spark.speculation", GlobalConfigUtils.sparkSpeculation)
      .set("spark.driver.allowMutilpleContext", GlobalConfigUtils.sparkDriverAllowMutilpleContext)
      .set("spark.serializer", GlobalConfigUtils.sparkSerializer)
      .set("spark.buffer.pageSize", GlobalConfigUtils.sparkBufferPageSize)

    //2、构建SparkSession对象
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //3、获取sparkContext对象
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("warn")

    //4、获取kuduContext对象
    val kuduContext = new KuduContext(GlobalConfigUtils.kuduMaster, sc)

    //5、处理业务逻辑
    //todo 5.1 根据ip解析对应的经度、维度、省、市，最后得到ODS层表的数据保存到kudu表中
//    ImproveData.process(sparkSession)

//    //todo: 5.2 统计各省市的地域分布情况
//    ProcessRegion.process(sparkSession)
//
//    //todo: 5.3 广告投放的地域分布情况统计
//    AdRegionAnalysis.process(sparkSession)
//
//    //todo: 5.4 广告投放的APP分布情况统计
//    AdAppAnalysis.process(sparkSession)
//
//    //todo: 5.5 广告投放的手机设备类型分布情况统计
//    AdDeviceAnalysis.process(sparkSession)
//
//    //todo: 5.6 广告投放的联网方式分布情况统计
//    AdNetWorkAnalysis.process(sparkSession)
//
//    //todo: 5.7 广告投放的运营商分布情况统计
//    AdIspnameAnalysis.process(sparkSession)
//
//    //todo: 5.8 广告投放的渠道分布情况统计
//    AdChannelAnalysis.process(sparkSession)
//
//    //todo: 5.9 生成商圈库
//    TradeAnalysis.process(sparkSession)

    //todo: 5.10  用户数据标签化
    DataTags.process(sparkSession)


    //关闭
    if (!sc.isStopped) {
      sc.stop()
    }

  }
}
