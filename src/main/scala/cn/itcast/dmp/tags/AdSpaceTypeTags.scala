package cn.itcast.dmp.tags

import cn.itcast.dmp.`trait`.Tags
import org.apache.spark.sql.Row

//广告位类型标签
object AdSpaceTypeTags  extends Tags{
  /**
    * 打标签的方法
    *
    * @param args 表示输入参数个数和类型任意
    * @return Map[String,Double]  key:String类型 表示标签的名称，value:Double 表示标签的权重
    */
  override def makeTags(args: Any*): Map[String, Double] = {
    var map=Map[String, Double]()

    if(args !=null && args.length>0){
      val row: Row = args(0).asInstanceOf[Row]
      //获取广告位类型
      val adspacetype: Long = row.getAs[Long]("adspacetype")
       //广告位类型（1：banner 2：插屏 3：全屏）

      adspacetype match {
        case 1 => map +=("LC@banner" ->1)
        case 2 => map +=("LC@插屏" ->1)
        case 3 => map +=("LC@全屏" ->1)
      }

    }

    map

  }
}
