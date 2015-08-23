package pool.core

/**
 * Created by Allen on 2015/8/22.
 */
trait SparkContextPool {
  def getOrCreate(allocationDesc: AllocationRequest): Option[Allocation]

  def release(id: Int)
}
