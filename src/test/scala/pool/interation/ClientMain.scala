package pool.interation

import java.io._

import org.apache.spark.serializer.{SerializationStream, DeserializationStream, SerializerInstance, JavaSerializer}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import pool.core.{SparkContextPoolServer, RemoteExecuteRDD}
import pool.messages.{ExecuteFailure, ExecuteSuccess, ReduceExecuteCommand}
import pool.serializer.{RDDJavaSerializer, RDDSerializer}
import pool.utils.Utils

import scala.collection.Map
import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.concurrent.duration._
import akka.pattern.{ask => akkaAsk}

/**
 * Created by Allen on 2015/8/23.
 */
object ClientMain {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Client")
    val rdd = new ParallelCollectionRDD(sc, 1 to 20, 2, Map.empty)
    println(rdd.reduce((a: Int, b: Int) => a + b))
  }
}

class ParallelCollectionRDD[T: ClassTag](
                                          @transient sc: SparkContext,
                                          @transient data: Seq[T],
                                          numSlices: Int,
                                          locationPrefs: Map[Int, Seq[String]])
  extends RDD[T](sc, Nil) {
  // TODO: Right now, each split sends along its full data, even if later down the RDD chain it gets
  // cached. It might be worthwhile to write the data to a file in the DFS and read it in the split
  // instead.
  // UPDATE: A parallel collection can be checkpointed to HDFS, which achieves this goal.

  override def getPartitions: Array[Partition] = {
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    new InterruptibleIterator(context, s.asInstanceOf[ParallelCollectionPartition[T]].iterator)
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }

  @transient val serializer: RDDSerializer = RDDJavaSerializer
  @transient val host = Utils.getLocalHost.getHostAddress
  @transient val serverPort = 9090
  @transient private val system = Utils.doCreateActorSystem(SparkContextPoolServer.serverActorSystemName,
    host, 9091)
  @transient private val actorSystem = system._1
  @transient val actorPath = s"akka.tcp://${SparkContextPoolServer.serverActorSystemName}@$host:$serverPort/user/${SparkContextPoolServer.serverActorName}"
  @transient val serverActor = Await.result(actorSystem.actorSelection(actorPath).resolveOne(20 second), 20 seconds)

  override def reduce(f: (T, T) => T): T = {
    val cmd = ReduceExecuteCommand(1, serializer.serialize(this).array(), f)
    Await.result(serverActor.ask(cmd)(20 second), Duration.Inf) match {
      case ExecuteSuccess(data) =>
        data.asInstanceOf[T]

      case ExecuteFailure(msg) =>
        sys.error(msg)
    }
  }
}

class ParallelCollectionPartition[T: ClassTag](
                                                var rddId: Long,
                                                var slice: Int,
                                                var values: Seq[T])
  extends Partition with Serializable {

  def iterator: Iterator[T] = values.iterator

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: ParallelCollectionPartition[_] =>
      this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = ParallelCollectionRDD.tryOrIOException {

    val sfactory = SparkEnv.get.serializer

    // Treat java serializer with default action rather than going thru serialization, to avoid a
    // separate serialization header.

    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeLong(rddId)
        out.writeInt(slice)

        val ser = sfactory.newInstance()
        ParallelCollectionRDD.serializeViaNestedStream(out, ser)(_.writeObject(values))
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = ParallelCollectionRDD.tryOrIOException {

    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        rddId = in.readLong()
        slice = in.readInt()

        val ser = sfactory.newInstance()
        ParallelCollectionRDD.deserializeViaNestedStream(in, ser)(ds => values = ds.readObject[Seq[T]]())
    }
  }
}

object ParallelCollectionRDD {

  def tryOrIOException(block: => Unit) {
    try {
      block
    } catch {
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
    }
  }

  def serializeViaNestedStream(os: OutputStream, ser: SerializerInstance)(
    f: SerializationStream => Unit): Unit = {
    val osWrapper = ser.serializeStream(new OutputStream {
      override def write(b: Int): Unit = os.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
    })
    try {
      f(osWrapper)
    } finally {
      osWrapper.close()
    }
  }

  def deserializeViaNestedStream(is: InputStream, ser: SerializerInstance)(
    f: DeserializationStream => Unit): Unit = {
    val isWrapper = ser.deserializeStream(new InputStream {
      override def read(): Int = is.read()
      override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
    })
    try {
      f(isWrapper)
    } finally {
      isWrapper.close()
    }
  }

  /**
   * Slice a collection into numSlices sub-collections. One extra thing we do here is to treat Range
   * collections specially, encoding the slices as other Ranges to minimize memory cost. This makes
   * it efficient to run Spark over RDDs representing large sets of numbers. And if the collection
   * is an inclusive Range, we use inclusive range for the last slice.
   */
  def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of slices required")
    }
    // Sequences need to be sliced at the same set of index positions for operations
    // like RDD.zip() to behave as expected
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map(i => {
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      })
    }
    seq match {
      case r: Range => {
        positions(r.length, numSlices).zipWithIndex.map({ case ((start, end), index) =>
          // If the range is inclusive, use inclusive range for the last slice
          if (r.isInclusive && index == numSlices - 1) {
            new Range.Inclusive(r.start + start * r.step, r.end, r.step)
          }
          else {
            new Range(r.start + start * r.step, r.start + end * r.step, r.step)
          }
        }).toSeq.asInstanceOf[Seq[Seq[T]]]
      }
      case nr: NumericRange[_] => {
        // For ranges of Long, Double, BigInteger, etc
        val slices = new ArrayBuffer[Seq[T]](numSlices)
        var r = nr
        for ((start, end) <- positions(nr.length, numSlices)) {
          val sliceSize = end - start
          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
          r = r.drop(sliceSize)
        }
        slices
      }
      case _ => {
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        positions(array.length, numSlices).map({
          case (start, end) =>
            array.slice(start, end).toSeq
        }).toSeq
      }
    }
  }
}
