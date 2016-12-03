package org.ldong.spark.sparkstreaming.redis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * @author cssdongl@gmail.com
  * @date 2016/12/3 16:19  
  * @version V1.0
  */
object RedisClient {
  val redisHost = "192.168.15.81"
  val redisPort = 6379
  val redisTimeout = 30000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)

}
