package oft_archipelago

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import oft_archipelago.Main.Start
import oft_archipelago.Node.{ABroadcast, RBroadcast}
import oft_archipelago.NodeGroup.RequestTrackDevice

object Main {
  val N = 20
  def apply(): Behavior[Command] =
    Behaviors.setup(context => new Main(context, 0))

  sealed trait Command
  final case class NodeRegistered(node: ActorRef[Node.Command]) extends Command
  final case class Start() extends Command
}

class Main(context: ActorContext[Main.Command], nodes: Int) extends AbstractBehavior[Main.Command](context) {
  import Main._
  override def onMessage(msg: Main.Command): Behavior[Main.Command] =
    msg match {
      case _ @ Start() =>
        val nodeGroup = context.spawn(NodeGroup(), "node-group")
        for(i <- 0 until N){
          nodeGroup ! RequestTrackDevice(i.toString, context.self)
        }
        this
      case _ @ NodeRegistered(node) =>
        context.log.info("New node created, total nodes created: {}", nodes + 1)
        if (nodes < N-1)
          new Main(context, nodes + 1)
        else {
          context.log.info("All nodes created")

          this
        }
    }
}

object ActorHierarchyExperiments extends App {
  val testSystem = ActorSystem(Main(), "testSystem")
  testSystem ! Start()
}
