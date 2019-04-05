package cn.itcast.dmp.tags

import cn.itcast.dmp.`trait`.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

//地域标签
object RegionTags  extends Tags{
  /**
    * 打标签的方法
    *
    * @param args 表示输入参数个数和类型任意
    * @return Map[String,Double]  key:String类型 表示标签的名称，value:Double 表示标签的权重
    */
  override def makeTags(args: Any*): Map[String, Double] = {
    var map= Map[String, Double]()
    if(args !=null && args.length>0){
      val row: Row = args(0).asInstanceOf[Row]

      //省
      val provincename: String = row.getAs[String]("provincename")
      if(StringUtils.isNotBlank(provincename)){
        map +=("PN@"+provincename -> 1)
      }

      //市
      val cityname: String = row.getAs[String]("cityname")
      if(StringUtils.isNotBlank(cityname)){
        map +=("CN@"+cityname -> 1)
      }

    }

    map
  }
}
