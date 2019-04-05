package cn.itcast.dmp.tools

import java.util

import org.apache.kudu.Schema
import org.apache.kudu.client.{CreateTableOptions, KuduClient}
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.DataFrame


// todo: 就是把dataFrame数据保存到kudu表中
object DBUtils {

  /**
    * 数据保存到kudu表中
    *
    * @param dataFrame   结果数据
    * @param kuduMaster  kudu集群地址
    * @param tableName   表名
    * @param schema      表结构信息
    * @param partitionID 表的分区字段
    */
  def saveData2Kudu(
                     dataFrame: DataFrame,
                     kuduMaster: String,
                     tableName: String,
                     schema: Schema,
                     partitionID: String
                   ): Unit = {
    val kuduClientBuilder: KuduClient.KuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMaster)
    val kuduClient: KuduClient = kuduClientBuilder.build()

    if (!kuduClient.tableExists(tableName)) {
      val options = new CreateTableOptions
      val partitionList = new util.ArrayList[String]()
      partitionList.add(partitionID) //按照哪个字段进行分区
      options.addHashPartitions(partitionList, 6)

      kuduClient.createTable(tableName, schema, options)
    }

    //把结果数据保存到kudu表中
    dataFrame.write.mode("append")
      .option("kudu.master", kuduMaster)
      .option("kudu.table", tableName)
      .kudu
  }

}
