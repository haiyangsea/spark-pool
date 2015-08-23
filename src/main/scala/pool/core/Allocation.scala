package pool.core

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListenerJobStart, SparkListener}
import scala.reflect.runtime.{universe => ru}
/**
 * Created by Allen on 2015/8/22.
 */
case class Allocation(id: Int, sparkContext: SparkContext, birth: Long, sharable: Boolean) {
  @volatile private var lastRunning = birth

  object SparkJobTracker extends SparkListener {
    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      lastRunning = System.currentTimeMillis()
    }
  }

  sparkContext.addSparkListener(SparkJobTracker)
  val mirror = ru.runtimeMirror(sparkContext.getClass.getClassLoader)
  val stoppedFiledName = "stopped"

  def breakDuration = System.currentTimeMillis() - lastRunning

  def isInvalid: Boolean = {
    val instance = mirror.reflect(sparkContext)
    val field = instance.symbol.typeSignature.member(ru.newTermName(stoppedFiledName)).asTerm
    field.accessed
    val result = instance.reflectField(field).get.asInstanceOf[AtomicBoolean]
    result.get()
  }
}
