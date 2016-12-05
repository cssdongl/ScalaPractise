package org.ldong.spark.sparkstreaming.redis

import org.ldong.spark.sparkstreaming.redis.utils.JedisUtil
import redis.clients.jedis._
import scala.collection.JavaConversions

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/4 10:45
  * @version V1.0
  */
class JedisTest(jedis: Jedis) {

  def testKey(): Unit = {
    jedis.set("Hello", "xee")
    println("test ends")
  }

  def testPoolMap(): Unit = {
    jedis.select(1)
    jedis.hset("hashs", "entryKey", "entryValue");
    jedis.hset("hashs", "entryKey1", "entryValue1");
    jedis.hset("hashs", "entryKey2", "entryValue2");
    jedis.hincrBy("app::users::click", "AXCWEA", 4)
  }
}

object JedisTest extends App {
  val conHost = "192.168.15.81"
  val conPort = 6379
  val jedis = JedisUtil.getInstance().getJedis(conHost, conPort)
  val instance = new JedisTest(jedis)
  instance.testKey()
  instance.testPoolMap()
  println(jedis.hexists("app::users::click", "AXCWEA"))
  println(jedis.hget("app::users::click","AXCWEA"))
  println(jedis.hget("hashs", "entryKey"));
  println(jedis.hmget("hashs", "entryKey", "entryKey1","entryKey2"));
  val map = jedis.hgetAll("app::users::click")
  val scalaMap = JavaConversions.mapAsScalaMap(map)
  for ((k, v) <- scalaMap) {
    println("k is " + k + "v is " + v)
  }
  JedisUtil.getInstance().closeJedis(jedis, conHost, conPort)
}