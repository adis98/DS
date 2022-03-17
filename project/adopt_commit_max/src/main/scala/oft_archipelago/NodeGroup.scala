package oft_archipelago

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, LoggerOps}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}
import oft_archipelago.Main.NodeRegistered
import oft_archipelago.Node.Stop

import scala.util.Random

object NodeGroup {
  def apply(): Behavior[Command] =
    Behaviors.setup(context => new NodeGroup(context))

  sealed trait Command

  private final case class NodeTerminated(node: ActorRef[Node.Command], nodeId: String) extends Command
  final case class BroadcastR(rBroadcast: Node.RBroadcast) extends Command
  final case class BroadcastA(aBroadcast: Node.ABroadcast) extends Command
  final case class BroadcastB(bBroadcast: Node.BBroadcast) extends Command
  final case class Start() extends Command
  final case class Commit(value: Int, nodeId: String) extends Command
  final case class RequestTrackDevice(nodeId: String, replyTo: ActorRef[Main.Command]) extends Command
}

class NodeGroup(context: ActorContext[NodeGroup.Command])
  extends AbstractBehavior[NodeGroup.Command](context) {
  import NodeGroup._

  private var nodeIdToActor = Map.empty[String, ActorRef[Node.Command]]

  context.log.info("NodeGroup started")

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case trackMsg @ RequestTrackDevice(nodeId, replyTo) =>
        nodeIdToActor.get(nodeId) match {
          case Some(nodeActor) =>
            replyTo ! NodeRegistered(nodeActor)
          case None =>
            context.log.info("Creating node actor for {}", trackMsg.nodeId)
            val nodeActor = context.spawn(Node(nodeId), s"node-$nodeId")
            nodeIdToActor += nodeId -> nodeActor
            context.log.info("Map Size {}", nodeIdToActor.size)
            if(nodeIdToActor.size > 19) {
              nodeIdToActor.foreach { case (_, nodeActor) =>
                val value = Random.nextInt(10)
                nodeActor ! Node.Start(value)
              }
            }
            replyTo ! NodeRegistered(nodeActor)
        }
        this
      case _ @ BroadcastR(rBroadcast) =>
        nodeIdToActor.foreach { case (_, nodeActor) =>
          if(nodeActor != rBroadcast.replyTo)
            nodeActor ! rBroadcast
        }
        this
      case _ @ BroadcastA(aBroadcast) =>
        nodeIdToActor.foreach { case (_, nodeActor) =>
          if(nodeActor != aBroadcast.replyTo)
          nodeActor ! aBroadcast
        }
        this
      case _ @ BroadcastB(bBroadcast) =>
        nodeIdToActor.foreach { case (_, nodeActor) =>
          if(nodeActor != bBroadcast.replyTo)
          nodeActor ! bBroadcast
        }
        this
      case _ @ Start() =>
        context.log.info("Starting Archipelago")
        this
      case _ @ Commit(value, nodeId) =>
        context.log.info("Received commit " + value.toString + " from " + nodeId)
        nodeIdToActor.foreach { case (_, nodeActor) =>
          nodeActor ! Stop()
        }
        Behaviors.stopped
    }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      context.log.info("NodeGroup stopped")
      this
  }
}
