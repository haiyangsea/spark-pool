package pool.serializer

import java.nio.ByteBuffer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
 * Created by Allen on 2015/8/23.
 */
trait RDDSerializer {
  def serialize[T: ClassTag](rdd: RDD[T]): ByteBuffer

  def deserialize[T: ClassTag](buffer: ByteBuffer, context: SparkContext): RDD[T]
}

