package oft_archipelago

import akka.actor.typed.scaladsl.adapter.TypedActorContextOps
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import oft_archipelago.Main.Start
import oft_archipelago.Node.{ABroadcast, RBroadcast}
import oft_archipelago.NodeGroup.RequestTrackDevice

object Main {
  val N = 20
  def apply(): Behavior[Command] =
    Behaviors.setup(context => new Main(context, 0, Map.empty[String, ActorRef[NodeGroup.Command]]))

  sealed trait Command
  final case class NodeRegistered(node: ActorRef[Node.Command]) extends Command
  final case class Start() extends Command
}

class Main(context: ActorContext[Main.Command], nodes: Int, idToActorGroup: Map[String, ActorRef[NodeGroup.Command]]) extends AbstractBehavior[Main.Command](context) {
  import Main._
  override def onMessage(msg: Main.Command): Behavior[Main.Command] =
    msg match {
      case _ @ Start() =>
        val nodeGroup = context.spawn(NodeGroup(), "node-group")
        var dupGroup = idToActorGroup
        dupGroup += "node-group" -> nodeGroup
        for(i <- 0 until N){
          nodeGroup ! RequestTrackDevice(i.toString, context.self)
        }
        new Main(context, nodes, dupGroup)
      case _ @ NodeRegistered(node) =>
        context.log.info("New node created, total nodes created: {}", nodes + 1)
        if (nodes < N-1)
          new Main(context, nodes + 1, idToActorGroup)
        else {
          context.log.info("Starting in main")
          this
        }
    }
}

object ActorHierarchyExperiments extends App {
  val testSystem = ActorSystem(Main(), "testSystem")
  testSystem ! Start()
}
