package cn.itcast.dmp.tools

import com.typesafe.config.{Config, ConfigFactory}

//todo:加载application.conf文件 获取配置属性和内容
object GlobalConfigUtils {

  private val config: Config = ConfigFactory.load()

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

  def sparkWorkerTimeout=config.getString("spark.worker.timeout")
  def sparkRpcAskTimeout=config.getString("spark.rpc.askTimeout")
  def sparkNetworkTimeout=config.getString("spark.network.timeout")
  def sparkCoresMax=config.getString("spark.cores.max")
  def sparkTaskMaxFailures=config.getString("spark.task.maxFailures")
  def sparkSpeculation=config.getString("spark.speculation")
  def sparkDriverAllowMutilpleContext=config.getString("spark.driver.allowMutilpleContext")
  def sparkSerializer=config.getString("spark.serializer")
  def sparkBufferPageSize=config.getString("spark.buffer.pageSize")

  //获取kudumaster的地址
  def kuduMaster=config.getString("kudu.master")


}
