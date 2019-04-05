package cn.itcast.dmp.tags

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import ch.hsr.geohash.GeoHash
import cn.itcast.dmp.`trait`.Tags
import cn.itcast.dmp.tools.GlobalConfigUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

//商圈标签
object TradeTags extends Tags{
  //定义impala的连接url地址
  val url=GlobalConfigUtils.impalaUrl

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
     //经度
      val longitude: String = row.getAs[String]("long")
      //纬度
      val latitude: String = row.getAs[String]("lat")

      //使用geohash编码得到经纬度的标识
      val geoHashCode: String = GeoHash.withCharacterPrecision(latitude.toDouble, longitude.toDouble, 8).toBase32

      //2种方式去查询得到商圈
        //  （1）sparksession----->dataFrame---->注册成表--->按照条件查询
        //   (2) impala--->按照条件查询

      //获取连接
      val connection: Connection = DriverManager.getConnection(url)
      //查询的sql
      val sql="select   trades  from trade where geohashcode=?"
      try {
        val ps: PreparedStatement = connection.prepareStatement(sql)
        ps.setString(1, geoHashCode)
        val rs: ResultSet = ps.executeQuery()

        while (rs.next()) {
          //得到了商圈信息
          val trades: String = rs.getString("trades")
          if (StringUtils.isNotBlank(trades)) {
            map += ("BA@" + trades -> 1)
          }
        }
      } catch {
        case e:Exception => println(e)
      } finally {
        if(connection !=null){
          connection.close()
        }
      }

    }


    map
  }
}
