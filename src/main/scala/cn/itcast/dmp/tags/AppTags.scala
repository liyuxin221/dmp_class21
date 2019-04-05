package cn.itcast.dmp.tags

import cn.itcast.dmp.`trait`.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

//app名称标签
object AppTags extends Tags{
  /**
    * 打标签的方法
    *
    * @param args 表示输入参数个数和类型任意
    * @return Map[String,Double]  key:String类型 表示标签的名称，value:Double 表示标签的权重
    */
  override def makeTags(args: Any*): Map[String, Double] = {
    var map=Map[String,Double]()

    if(args !=null && args.length>1){
      val row: Row = args(0).asInstanceOf[Row]
      val appMap: Map[String, String] = args(1).asInstanceOf[Map[String,String]]

      //获取appid
      val appid: String = row.getAs[String]("appid")
      var appname: String = row.getAs[String]("appname")
           //appMap--->key:appid   value:appname
      if(!StringUtils.isNotBlank(appname)){
        appname = appMap.getOrElse(appid,"no appname")
      }

      map +=("APP@"+appname ->1)

    }

    map
  }
}
