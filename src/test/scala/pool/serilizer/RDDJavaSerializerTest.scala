package pool.serilizer

import org.apache.spark.SparkContext
import org.scalatest.{FunSuite, Matchers}
import pool.serializer.RDDJavaSerializer

/**
 * Created by Allen on 2015/8/23.
 */
class RDDJavaSerializerTest extends FunSuite with Matchers {
  test("first") {
    val sc = new SparkContext("local[4]", "Serializer Test")
    val rdd = sc.parallelize(1 to 20).map((_, 1)).groupByKey()

    val buffer = RDDJavaSerializer.serialize(rdd)
    val nRDD = RDDJavaSerializer.deserialize[(Int, Iterable[Int])](buffer, sc)

    val f = (x: (Int, Iterable[Int]), y: (Int, Iterable[Int])) => {
      (x._1 + y._1, x._2 ++ y._2)
    }

    val reducePartition: Iterator[(Int, Iterable[Int])] => Option[(Int, Iterable[Int])] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(f))
      } else {
        None
      }
    }

    var jobResult: Option[(Int, Iterable[Int])] = None
    val mergeResult = (index: Int, taskResult: Option[(Int, Iterable[Int])]) => {
      if (taskResult.isDefined) {
        jobResult = jobResult match {
          case Some(value) => Some(f(value, taskResult.get))
          case None => taskResult
        }
      }
    }

    sc.runJob(nRDD, reducePartition, mergeResult)

    if (jobResult.isDefined) {
      val result = jobResult.get
      val value = result._1 + result._2.reduce(_ + _)
      val result1 = rdd.reduce((x, y) => (x._1 + y._1, x._2 ++ y._2))
      val value1 = result1._1 + result1._2.reduce(_ + _)

      (value == value1) should be (true)
    } else {
      fail("there should have result!")
    }
  }
}
