package oft_archipelago

import akka.actor.TypedActor.context
import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import oft_archipelago.Main.Start
import oft_archipelago.NodeGroup.RequestTrackDevice

object Main {

  def apply(): Behavior[Command] =
    Behaviors.setup(context => new Main(context, 0))

  sealed trait Command

  final case class NodeRegistered(node: ActorRef[Node.Command]) extends Command

  final case class Start(n: Int,f: Double) extends Command
}

class Main(context: ActorContext[Main.Command], nodes: Int) extends AbstractBehavior[Main.Command](context) {

  import Main._

  override def onMessage(msg: Main.Command): Behavior[Main.Command] =
    msg match {
      case _@Start(n: Int,f: Double) =>
        val nodeGroup = context.spawn(NodeGroup(), "node-group")
        nodeGroup ! NodeGroup.Start(f)
        for (i <- 0 until n) {
          nodeGroup ! RequestTrackDevice(i.toString, n, context.self)
        }
        new Main(context, nodes)
      case _@NodeRegistered(node) =>
//        context.log.info("New node created, total nodes created: {}", nodes + 1)
        this
    }
}

object ActorHierarchyExperiments extends App {
  val testSystem = ActorSystem(Main(), "testSystem")
  testSystem ! Start(80,0)
}

object ExperimentalRuns extends App {
  val disabledPercPerRound = Set[Double](0.1,0.2,0.3,0.4)
  val procsPerRound= Set[Int](8,4,16)
  val scenarioRuns=30
  var id=0
  procsPerRound.foreach{ case(procsNum) =>
    disabledPercPerRound.foreach{ case(disabledPerc) =>
      for( i <- 1 to scenarioRuns-1)
        {

          val testSystem = ActorSystem(Main(), "testSystem"+id.toString)
//          testSystem.log.info("Executing run "+i.toString+" for n="+procsNum.toString+" and f="+disabledPerc.toString)
          testSystem ! Start(procsNum,disabledPerc)
          id+=1
          procsNum match{
            case 128 =>Thread.sleep(20000)
            case 64 => Thread.sleep(10000)
            case _ => Thread.sleep(5000)
          }

        }
        id+=1
    }
    id+=1
  }
}
