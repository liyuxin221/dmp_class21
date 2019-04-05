package cn.itcast.dmp.tags

import cn.itcast.dmp.`trait`.Tags
import org.apache.spark.sql.Row

//年龄标签
object AgeTags  extends Tags{
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
      //获取年龄
      val age: String = row.getAs[String]("age")

      map +=("AGE@"+age -> 1)

    }

    map

  }
}
