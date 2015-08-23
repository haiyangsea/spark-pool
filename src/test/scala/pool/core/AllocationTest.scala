package pool.core

import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import org.scalatest.Matchers
import pool.messages.ReduceExecuteCommand

/**
 * Created by Allen on 2015/8/22.
 */
class AllocationTest extends FunSuite with Matchers {
//  test("isRunning") {
//    val response = new Allocation(new SparkContext("local", "test"), System.currentTimeMillis(), true)
//
//    response.isInvalid should be (false)
//
//    response.sparkContext.stop()
//
//    response.isInvalid should be (true)
//
//  }
//
//  test("breakDuration") {
//    val response = new Allocation(new SparkContext("local", "test0"), System.currentTimeMillis(), true)
//    Thread.sleep(1000)
//
//    (response.breakDuration >= 1000) should be (true)
//
//    response.sparkContext.parallelize(1 to 20).collect()
//    Thread.sleep(1000)
//
//    (response.breakDuration >= 2000) should be (false)
//    (response.breakDuration >= 1000) should be (true)
//  }

  test("T") {
    val cmd = ReduceExecuteCommand(1, null, (a: Int, b: Int) => a + b)

    cmd match {
      case c: ReduceExecuteCommand[x] =>
        println(c)
    }
  }
}
