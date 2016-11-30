package org.ldong.scala.practise.chapter8

/**
  * @author cssdongl@gmail.com
  * @date 2016/11/25 11:25  
  * @version V1.0
  */
class CheckAcount(initialBalance: Double) extends BankAcount(initialBalance) {
  private var balance: Double = initialBalance

  override def deposit(amount: Double): Double = super.deposit(amount - 1)

  override def withDraw(amount: Double): Double = {
    super.withDraw(amount - 1)
  }
}
