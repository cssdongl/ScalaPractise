package org.ldong.scala.actor
import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging
/**
  * 描述:
  * test actor receive
  *
  * @author dongliang
  * @create 2018-02-14 10:34
  */
class MyActorReceive extends Actor{

  val log = Logging(context.system, this)

  override def receive: Receive = {
    case "test" => log.info("received test")
    case _ => log.info("received unknown message")
  }

  val system = ActorSystem("MyActorSystem")

  val systemLog = system.log

  val myActor = system.actorOf(Props[MyActorReceive], name="myactor")

  systemLog.info("prepare send message to myactor")

  myActor!"test"

  myActor!123

  system.shutdown()
}


