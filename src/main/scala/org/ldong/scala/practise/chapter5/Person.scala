package org.ldong.scala.practise.chapter5

/**
  * @author dongliang@mail.jj.com
  * @date 2016/11/24 14:52  
  * @version V1.0
  */
class Person(var age: Int) {
  age = if(age < 0) 0 else age
}
