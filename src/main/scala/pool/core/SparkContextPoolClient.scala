package pool.core

/**
 * Created by Allen on 2015/8/23.
 */
class SparkContextPoolClient(host: String, port: Int) extends SparkContextPool {
  override def getOrCreate(allocationDesc: AllocationRequest): Option[Allocation] = ???

  override def release(id: Int): Unit = ???
}
