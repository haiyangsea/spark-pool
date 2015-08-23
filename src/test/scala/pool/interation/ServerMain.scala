package pool.interation

import pool.core.SparkContextPoolServer

/**
 * Created by Allen on 2015/8/23.
 */
object ServerMain {
  def main(args: Array[String]) {
    val server = new SparkContextPoolServer
    server.start()
  }
}
