package oft_archipelago

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import oft_archipelago.Main.Start
import oft_archipelago.NodeGroup.RequestTrackDevice

object Main {

  def apply(): Behavior[Command] =
    Behaviors.setup(context => new Main(context, 0))

  sealed trait Command

  final case class NodeRegistered(node: ActorRef[Node.Command]) extends Command

  final case class Start(n: Int) extends Command
}

class Main(context: ActorContext[Main.Command], nodes: Int) extends AbstractBehavior[Main.Command](context) {

  import Main._

  override def onMessage(msg: Main.Command): Behavior[Main.Command] =
    msg match {
      case _@Start(n: Int) =>
        val nodeGroup = context.spawn(NodeGroup(), "node-group")
        nodeGroup ! NodeGroup.Start()
        for (i <- 0 until n) {
          nodeGroup ! RequestTrackDevice(i.toString, n, context.self)
        }
        new Main(context, nodes)
      case _@NodeRegistered(node) =>
        context.log.info("New node created, total nodes created: {}", nodes + 1)
        this
    }
}

object ActorHierarchyExperiments extends App {
  val testSystem = ActorSystem(Main(), "testSystem")
  testSystem ! Start(125)
}
