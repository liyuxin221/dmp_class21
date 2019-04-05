package cn.itcast.dmp.`trait`

//todo:打标签的接口
trait Tags {
  /**
    * 打标签的方法
    * @param args 表示输入参数个数和任意类型
    * @return key:String类型 表示标签的名称,  value:Double 表示标签的权重
    */
  def makeTags(args:Any*):Map[String,Double]
}
