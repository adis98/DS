package oft_archipelago

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, LoggerOps}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}
import oft_archipelago.Main.NodeRegistered

object NodeGroup {
  def apply(): Behavior[Command] =
    Behaviors.setup(context => new NodeGroup(context))

  sealed trait Command

  private final case class NodeTerminated(node: ActorRef[Node.Command], nodeId: String) extends Command
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
            replyTo ! NodeRegistered(nodeActor)
        }
        this
    }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      context.log.info("NodeGroup stopped")
      this
  }
}
