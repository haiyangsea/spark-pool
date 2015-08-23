package pool.serializer

import java.io._
import java.nio.ByteBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.{ShuffleDependency, SparkContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
/**
 * Created by Allen on 2015/8/23.
 */
object RDDJavaSerializer extends RDDSerializer {
  private val mirror = ru.runtimeMirror(SparkContext.getClass.getClassLoader)
  private val partitionsFieldName = "partitions_"
  private val sparkContextFieldName = "_sc"
  private val shuffleRDDField = "_rdd"
  private val callSiteFieldName = "creationSite"

  private val rddType = ru.typeOf[RDD[_]].typeSymbol.typeSignature
  private val partitionsField = rddType.member(ru.newTermName(partitionsFieldName)).asMethod.setter.asMethod
  private val sparkContextField = rddType.member(ru.newTermName(sparkContextFieldName)).asTerm
  private val callSiteField = rddType.member(ru.newTermName(callSiteFieldName)).asTerm

  override def serialize[T: ClassTag](rdd: RDD[T]): ByteBuffer = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)

    serialize(rdd, oos)
    oos.close()
    ByteBuffer.wrap(baos.toByteArray)
  }

  private def visit[T](rdd: RDD[T]): Unit = {
    rdd.dependencies.foreach(dep => visit(dep.rdd))
  }

  private def serialize[T: ClassTag](rdd: RDD[T], oos: ObjectOutputStream): Unit = {
    visit(rdd)
    oos.writeObject(rdd)
    writeOthers(rdd, oos)
  }

  private def writeOthers[T: ClassTag](rdd: RDD[T], oos: ObjectOutputStream): Unit = {
    oos.writeObject(rdd.partitions)
    val callsite = mirror.reflect(rdd).reflectField(callSiteField).get
    oos.writeObject(callsite)
    rdd.dependencies.foreach {
      case dep: ShuffleDependency[_, _, _] =>
        serialize(dep.rdd, oos)

      case dep =>
        writeOthers(dep.rdd, oos)
    }
  }

  override def deserialize[T: ClassTag](buffer: ByteBuffer, context: SparkContext): RDD[T] = {
    val bois = new ByteArrayInputStream(buffer.array())
    val ois = new ObjectInputStream(bois)

    val rdd  = deserialize[T](ois, context)
    ois.close()

    rdd
  }

  private def deserialize[T: ClassTag](ois: ObjectInputStream, context: SparkContext): RDD[T] = {
    val rdd = ois.readObject().asInstanceOf[RDD[T]]
    readOthers(rdd, ois, context)
    rdd
  }

  private def readOthers[T: ClassTag](rdd: RDD[T], ois: ObjectInputStream, context: SparkContext): Unit = {
    val m = mirror.reflect(rdd)
    m.reflectMethod(partitionsField).apply(ois.readObject())
    m.reflectField(sparkContextField).set(context)
    val callsite = ois.readObject()
    m.reflectField(callSiteField).set(callsite)

    rdd.dependencies.foreach {
      case dep: ShuffleDependency[_, _, _] =>
        val d = mirror.reflect(dep)
        val _rdd = d.symbol.typeSignature.member(ru.newTermName(shuffleRDDField)).asTerm
        d.reflectField(_rdd).set(deserialize(ois, context))

      case dep =>
        readOthers(dep.rdd, ois, context)
    }
  }
}
