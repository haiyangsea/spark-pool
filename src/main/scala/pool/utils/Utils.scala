package pool.utils

import java.net.{Inet4Address, NetworkInterface, InetAddress}

import akka.actor.{ExtendedActorSystem, ActorSystem}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConversions._

/**
 * Created by Allen on 2015/8/23.
 */
object Utils {
  // TODO : make right config
  def doCreateActorSystem(
     name: String,
     host: String,
     port: Int): (ActorSystem, Int) = {

    val akkaThreads = 4
    val akkaBatchSize = 15
    val akkaTimeoutS = "120"
    val akkaLogLifecycleEvents = false
    val lifecycleEvents = if (akkaLogLifecycleEvents) "on" else "off"
    if (!akkaLogLifecycleEvents) {
      // As a workaround for Akka issue #3787, we coerce the "EndpointWriter" log to be silent.
      // See: https://www.assembla.com/spaces/akka/tickets/3787#/
      Option(Logger.getLogger("akka.remote.EndpointWriter")).map(l => l.setLevel(Level.FATAL))
    }

    val logAkkaConfig = "off"

    val akkaHeartBeatPausesS = "6000"
    val akkaHeartBeatIntervalS = "1000"

    val requireCookie = "off"
    val secureCookie = ""

    val akkaConf = ConfigFactory.parseString(
      s"""
         |akka.daemonic = on
         |akka.loggers = [""akka.event.slf4j.Slf4jLogger""]
         |akka.stdout-loglevel = "ERROR"
         |akka.jvm-exit-on-fatal-error = off
         |akka.remote.require-cookie = "$requireCookie"
         |akka.remote.secure-cookie = "$secureCookie"
         |akka.remote.transport-failure-detector.heartbeat-interval = $akkaHeartBeatIntervalS s
         |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = $akkaHeartBeatPausesS s
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = $port
         |akka.remote.netty.tcp.tcp-nodelay = on
         |akka.remote.netty.tcp.connection-timeout = $akkaTimeoutS s
         |akka.remote.netty.tcp.execution-pool-size = $akkaThreads
         |akka.actor.default-dispatcher.throughput = $akkaBatchSize
         |akka.log-config-on-start = $logAkkaConfig
         |akka.remote.log-remote-lifecycle-events = $lifecycleEvents
         |akka.log-dead-letters = $lifecycleEvents
         |akka.log-dead-letters-during-shutdown = $lifecycleEvents
      """.stripMargin)

    val actorSystem = ActorSystem(name, akkaConf)
    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val boundPort = provider.getDefaultAddress.port.get
    (actorSystem, boundPort)
  }

  val isWindows = {
    val name = System.getProperty("os.name")
    Option(name).getOrElse("").startsWith("Windows")
  }
  val getLocalHost: InetAddress = {
    val address = InetAddress.getLocalHost

//    if (address.isLoopbackAddress) {
//      // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
//      // a better address using the local network interfaces
//      // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
//      // on unix-like system. On windows, it returns in index order.
//      // It's more proper to pick ip address following system output order.
//      val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.toList
//      val reOrderedNetworkIFs = if (isWindows) activeNetworkIFs else activeNetworkIFs.reverse
//
//      for (ni <- reOrderedNetworkIFs) {
//        val addresses = ni.getInetAddresses.toList
//          .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress)
//        if (addresses.nonEmpty) {
//          val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
//          // because of Inet6Address.toHostName may add interface at the end if it knows about it
//          val strippedAddress = InetAddress.getByAddress(addr.getAddress)
//          // We've found an address that looks reasonable!
//          strippedAddress
//        }
//      }
//    } else {
//      address
//    }
    address
  }
}
