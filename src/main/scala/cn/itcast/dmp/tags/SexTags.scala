package cn.itcast.dmp.tags

import cn.itcast.dmp.`trait`.Tags
import org.apache.spark.sql.Row

//性别标签
object SexTags extends Tags {
  /**
    * 打标签的方法
    *
    * @param args 表示输入参数个数和任意类型
    * @return key:String类型 表示标签的名称,  value:Double 表示标签的权重
    */
  override def makeTags(args: Any*): Map[String, Double] = {
    var map = Map[String, Double]()

    if (args.length > 0) {
      val row: Row = args(0).asInstanceOf[Row]
      val sex: String = row.getAs[String]("sex")

      sex match {
        case "0" => map += ("SEX@" + "男" -> 1)
        case "1" => map += ("SEX@" + "女" -> 1)
      }
    }


    map
  }
}
