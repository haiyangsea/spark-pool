package pool.core

import org.apache.spark.SparkConf

import scala.concurrent.duration.Duration

/**
 * Created by Allen on 2015/8/22.
 */
case class AllocationRequest(cores: Int, memory: Int, sharable: Boolean, timeout: Duration, conf: Map[String, String])
