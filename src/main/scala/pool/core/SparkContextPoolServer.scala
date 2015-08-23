package pool.core

import java.nio.ByteBuffer

import akka.actor.{Props, Actor}
import akka.actor.Actor.Receive
import org.apache.spark.SparkContext
import pool.messages.{ExecuteFailure, ExecuteSuccess, ReduceExecuteCommand}
import pool.serializer.{RDDJavaSerializer, RDDSerializer}
import pool.utils.Utils

import scala.reflect.ClassTag

/**
 * Created by Allen on 2015/8/23.
 */
class SparkContextPoolServer extends StandartSparkContextPool {

  val serializer: RDDSerializer = RDDJavaSerializer
  val sc = new SparkContext("local[4]", "Server App")

  private val (actorSystem, finalPort) = Utils.doCreateActorSystem(SparkContextPoolServer.serverActorSystemName,
    Utils.getLocalHost.getHostAddress, 9090)

  def start(): Unit = {
    println("starting server....")
    actorSystem.actorOf(Props(new ServerActor), SparkContextPoolServer.serverActorName)
    actorSystem.awaitTermination()
  }

  def stop(): Unit = {

    actorSystem.shutdown()
  }

  val host: String = Utils.getLocalHost.getHostName

  val port: Int = finalPort

  class ServerActor extends Actor {
    override def receive: Receive = {
      case cmd: ReduceExecuteCommand[Int] =>
        val rdd = serializer.deserialize[Int](ByteBuffer.wrap(cmd.data), sc)
        val reducePartition: Iterator[Int] => Option[Int] = iter => {
          if (iter.hasNext) {
            Some(iter.reduceLeft(cmd.f))
          } else {
            None
          }
        }
        var jobResult: Option[Int] = None
        val mergeResult = (index: Int, taskResult: Option[Int]) => {
          if (taskResult.isDefined) {
            jobResult = jobResult match {
              case Some(value) => Some(cmd.f(value, taskResult.get))
              case None => taskResult
            }
          }
        }

        sc.runJob(rdd, reducePartition, mergeResult)
        // Get the final result out of our Option, or throw an exception if the RDD was empty
        if (jobResult.isDefined) {
          println("compute result : " + jobResult.get)
          sender ! ExecuteSuccess(jobResult.get)
        } else {
          sender ! ExecuteFailure("error when execute")
        }
    }
  }
}

object SparkContextPoolServer {
  val serverActorSystemName = "sparkContextPoolServer"
  val serverActorName = "serverActor"
}
