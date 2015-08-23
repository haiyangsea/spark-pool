package pool.core

import org.apache.spark.rdd.RDD
import pool.messages.{ExecuteFailure, ExecuteSuccess, ReduceExecuteCommand}
import pool.serializer.{RDDJavaSerializer, RDDSerializer}
import pool.utils.Utils
import akka.pattern.{ask => akkaAsk}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
 * Created by Allen on 2015/8/23.
 */
trait RemoteExecuteRDD[Int] extends RDD[Int] {
  val serializer: RDDSerializer = RDDJavaSerializer
  val host = Utils.getLocalHost.getHostAddress
  val serverPort = 9090
  private val (actorSystem, finalPort) = Utils.doCreateActorSystem(SparkContextPoolServer.serverActorSystemName,
    host, 9090)

  val actorPath = s"akka.tcp://${SparkContextPoolServer.serverActorSystemName}@$host:$serverPort/user/${SparkContextPoolServer.serverActorName}"
  val serverActor = Await.result(actorSystem.actorSelection(actorPath).resolveOne(20 second), 20 seconds)

//  override def reduce(f: (Int, Int) => Int): Int = {
//    val cmd = ReduceExecuteCommand(1, serializer.serialize(this).array(), f)
//    Await.result(serverActor.ask(cmd)(20 second), 20 second) match {
//      case ExecuteSuccess(data) =>
//        data.asInstanceOf[Int]
//
//      case ExecuteFailure(msg) =>
//        sys.error(msg)
//    }
//  }
}
