package pool.messages

import scala.reflect.ClassTag

/**
 * Created by Allen on 2015/8/23.
 */
case class ReduceExecuteCommand[T](id: Int, data: Array[Byte], f: (T, T) => T)

sealed abstract class ExecuteResult

case class ExecuteSuccess(data: Any) extends ExecuteResult

case class ExecuteFailure(message: String) extends ExecuteResult
