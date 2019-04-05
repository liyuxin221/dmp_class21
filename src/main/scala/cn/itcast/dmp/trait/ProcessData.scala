package cn.itcast.dmp.`trait`

import org.apache.spark.sql.SparkSession


// todo:处理数据的接口
trait ProcessData {

  /**
    * 处理数据,实现不同的ETL操作
    * @param sparkSession
    */
  def process(sparkSession: SparkSession)
}
