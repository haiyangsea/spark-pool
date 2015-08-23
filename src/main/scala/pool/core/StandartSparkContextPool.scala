package pool.core

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Allen on 2015/8/23.
 */
class StandartSparkContextPool extends SparkContextPool {
  val sparkPool = new ConcurrentHashMap[Int, Allocation]()
  val idGenerator = new AtomicInteger(0)

  private val allowMultiConf = "spark.driver.allowMultipleContexts"

  override def getOrCreate(allocationDesc: AllocationRequest): Option[Allocation] = {
    val sc = createSparkContext(allocationDesc)
    val id = idGenerator.getAndIncrement()
    val allocation = Allocation(id, sc, System.currentTimeMillis(), allocationDesc.sharable)
    sparkPool.put(id, allocation)
    Some(allocation)
  }

  override def release(id: Int): Unit = {
    if (sparkPool.contains(id)) {
      sparkPool.get(id).sparkContext.stop()
      sparkPool.remove(id)
    }
  }

  private def createSparkContext(desc: AllocationRequest): SparkContext = {
    val conf = new SparkConf().setAll(desc.conf)
    conf.set(allowMultiConf, "true")

    return new SparkContext(conf)
  }
}
