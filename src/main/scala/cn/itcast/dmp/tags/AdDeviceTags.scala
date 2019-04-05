package cn.itcast.dmp.tags

import cn.itcast.dmp.`trait`.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

//设备类型标签
object AdDeviceTags  extends Tags{
  /**
    * 打标签的方法
    *
    * @param args 表示输入参数个数和类型任意
    * @return Map[String,Double]  key:String类型 表示标签的名称，value:Double 表示标签的权重
    */
  override def makeTags(args: Any*): Map[String, Double] = {
    var map=Map[String, Double]()
    if(args !=null && args.length >1){

      val row: Row = args(0).asInstanceOf[Row]
      val deviceMap: Map[String, String] = args(1).asInstanceOf[Map[String, String]]


      //设备类型(手机操作系统、联网方式、运营商）

      //手机操作系统
      val client: Long = row.getAs[Long]("client")
      if(client !=null){
          //1:ios  //2:android   //3:wp   //4:other
        val os: String = deviceMap.getOrElse(client.toString,"D00010004")
        map +=("OS@"+os -> 1)
      }


      //联网方式
      val networkmannername: String = row.getAs[String]("networkmannername")
      if(StringUtils.isNotBlank(networkmannername)){
        val network: String = deviceMap.getOrElse(networkmannername,"D00020005")
        map +=("NETWORK@"+network -> 1)
      }


      //运营商
      val ispname: String = row.getAs[String]("ispname")
      if(StringUtils.isNotBlank(ispname)){
        val isp: String = deviceMap.getOrElse(ispname,"D00030004")
        map +=("ISP@"+isp -> 1)
      }

    }

    map
  }
}
