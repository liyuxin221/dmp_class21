package cn.itcast.dmp.tools

import java.util.Date

import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.FastDateFormat


//todo:时间处理的工具类
object DateUtils {


  /**
    * 获取当天时间
    * @return 日期 20190401
    */
  def getNowDay:String={
    val date = new Date
    //SimapleDataFormat它是线程不安全，这里的FastDateFormat它是线程安全
    val instance: FastDateFormat = FastDateFormat.getInstance("yyyyMMdd HH:mm:ss")

    val now: String = instance.format(date)

    var time="no time"
    if(StringUtils.isNotBlank(now)){
      time=  now.split(" ")(0)
    }
    time
  }
}
