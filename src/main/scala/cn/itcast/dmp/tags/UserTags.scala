package cn.itcast.dmp.tags

import java.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

// 用户的标识标签
object UserTags {

  def makeTags(row: Row) = {

    //定义一个list集合，用户存储用户的所有标识
    val list = new util.LinkedList[String]()

    //获取imei
    val imei: String = row.getAs[String]("imei")
    if (StringUtils.isNotBlank(imei)) {
      list.add("imei:".toUpperCase+ imei)
    }

    //获取mac
    val mac: String = row.getAs[String]("mac")
    if (StringUtils.isNotBlank(mac)) {
      list.add("mac:".toUpperCase + mac)
    }

    //获取idfa
    val idfa: String = row.getAs[String]("idfa")
    if (StringUtils.isNotBlank("idfa")) {
      list.add("idfa:".toUpperCase+ idfa)
    }

    //获取openudid
    val openudid: String = row.getAs[String]("openudid")
    if(StringUtils.isNotBlank(openudid)){
      list.add("openudid:".toUpperCase+openudid)
    }

    //获取androidid
    val androidid: String = row.getAs[String]("androidid")
    if(StringUtils.isNotBlank(androidid)){
      list.add("androidid:".toUpperCase+androidid)
    }

    list
  }
}
