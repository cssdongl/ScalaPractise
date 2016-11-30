package org.ldong.scala.practise.chapter5

import scala.beans.BeanProperty

/**
  * @author dongliang@mail.jj.com
  * @date 2016/11/24 14:47  
  * @version V1.0
  */
class Student() {
  @BeanProperty var name:String = _
  @BeanProperty var id:Long = _
}
