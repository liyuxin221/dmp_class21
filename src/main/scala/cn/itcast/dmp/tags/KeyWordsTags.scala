package cn.itcast.dmp.tags

import cn.itcast.dmp.`trait`.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

//关键字标签
object KeyWordsTags extends Tags{
  /**
    * 打标签的方法
    *
    * @param args 表示输入参数个数和类型任意
    * @return Map[String,Double]  key:String类型 表示标签的名称，value:Double 表示标签的权重
    */
  override def makeTags(args: Any*): Map[String, Double] = {
    var map=Map[String, Double]()
    if(args !=null && args.length >0){
      val row: Row = args(0).asInstanceOf[Row]
        // 漫画,火影忍者,热血动漫,大蛇丸,宇智波佐助
      val keywords: String = row.getAs[String]("keywords")

      if(StringUtils.isNotBlank(keywords)){
        val keywordsArray: Array[String] = keywords.split(",")

        keywordsArray.foreach(k => map +=("K@"+k ->1))

      }

    }

    map
  }
}
