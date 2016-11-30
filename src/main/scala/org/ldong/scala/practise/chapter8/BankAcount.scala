package org.ldong.scala.practise.chapter8

/**
  * @author cssdongl@gmail.com
  * @date 2016/11/25 11:22  
  * @version V1.0
  */
class BankAcount(inititalBalance:Double) {
  private var balance:Double = inititalBalance
  def deposit(amount:Double)  = {
    balance += amount
    balance
  }
  def withDraw(amount:Double) = {
    balance -= amount
    balance
  }
}
