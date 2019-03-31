package cn.itcast.dmp

import cn.itcast.dmp.tools.GlobalConfigUtils
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

//todo:项目的执行入口，后期的大量处理逻辑都在这个object
object DmpMain {
  def main(args: Array[String]): Unit = {

    /**
spark.worker.timeout="500"
spark.rpc.askTimeout="600s"
spark.network.timeout="600s"
spark.cores.max="10"
spark.task.maxFailures="5"
spark.speculation="true"
spark.driver.allowMutilpleContext="true"
spark.serializer="org.apache.spark.serializer.KryoSerializer"
spark.buffer.pageSize="8m"
      */
     //1、构建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("DmpMain")
                                              .setMaster("local[6]")
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
      val kuduContext = new KuduContext(GlobalConfigUtils.kuduMaster,sc)


  }
}
