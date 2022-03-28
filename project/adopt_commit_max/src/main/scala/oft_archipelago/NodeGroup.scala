package oft_archipelago

import akka.actor.ActorPath
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, LoggerOps}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}
import oft_archipelago.Main.NodeRegistered
import oft_archipelago.Node.Stop

import scala.collection.mutable
import scala.util.Random

object NodeGroup {
  def apply(): Behavior[Command] =
    Behaviors.setup(context => new NodeGroup(context))

  sealed trait Command

  final case class RequestTrackDevice(nodeId: String, replyTo: ActorRef[Main.Command]) extends Command
  final case class Start() extends Command
  final case class Commit(value: Int, nodeId: String) extends Command
  final case class BroadcastR(rBroadcast: Node.RBroadcast) extends Command
  final case class BroadcastA(aBroadcast: Node.ABroadcast) extends Command
  final case class BroadcastB(bBroadcast: Node.BBroadcast) extends Command
}

class NodeGroup(context: ActorContext[NodeGroup.Command])
  extends AbstractBehavior[NodeGroup.Command](context) {
  import NodeGroup._

  private var nodeIdToActor = Map.empty[String, ActorRef[Node.Command]]
  private var disabledProcs = Map.empty[Int,Set[String]]
  private var round= -1

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
        if(rBroadcast.i>round){
          round=rBroadcast.i
          blockNewProcesses(round)
        }
        val procsBlockedInRound=disabledProcs.get(rBroadcast.i)
        if(!procsBlockedInRound.get.contains(rBroadcast.replyTo.path.name)){
          nodeIdToActor.foreach { case (_, nodeActor) =>
            if(nodeActor != rBroadcast.replyTo)
              if(!procsBlockedInRound.get.contains(nodeActor.path.name)){
                nodeActor ! rBroadcast
              }else{
                context.log.info("Process {} was blocked from receiving R-step message", nodeActor)
              }
          }
        }else{
          context.log.info("Process {} was blocked from sending R-step message", rBroadcast.replyTo)
        }
        this
      case _ @ BroadcastA(aBroadcast) =>
        if(aBroadcast.i>round){
          round=aBroadcast.i
          blockNewProcesses(round)
        }
        val procsBlockedInRound=disabledProcs.get(aBroadcast.i)
        if(!procsBlockedInRound.get.contains(aBroadcast.replyTo.path.name)) {
          nodeIdToActor.foreach { case (_, nodeActor) =>
            if(nodeActor != aBroadcast.replyTo)
            if(!procsBlockedInRound.get.contains(nodeActor.path.name)) {
              nodeActor ! aBroadcast
            }else{
              context.log.info("Process {} was blocked from receiving A-step message", nodeActor)
            }
          }
        }else{
          context.log.info("Process {} was blocked from sending A-step message", aBroadcast.replyTo)
        }
        this
      case _ @ BroadcastB(bBroadcast) =>
        if(bBroadcast.i>round){
          round=bBroadcast.i
          blockNewProcesses(round)
        }
        val procsBlockedInRound=disabledProcs.get(bBroadcast.i)
        if(!procsBlockedInRound.get.contains(bBroadcast.replyTo.path.name)) {
          nodeIdToActor.foreach { case (_, nodeActor) =>
            if(nodeActor != bBroadcast.replyTo)
            if(!procsBlockedInRound.get.contains(nodeActor.path.name)) {
              nodeActor ! bBroadcast
            }else{
              context.log.info("Process {} was blocked from receiving B-step message", nodeActor)
            }
          }
        }else{
          context.log.info("Process {} was blocked from sending B-step message", bBroadcast.replyTo)
        }
        this
      case _ @ Start() =>
        context.log.info("Starting Archipelago")
        this
      case _ @ Commit(value, nodeId) =>
        context.log.info("Received commit " + value.toString + " from " + nodeId)
        this
    }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
      context.log.info("NodeGroup stopped")
      this
  }

  def blockNewProcesses(round: Int): Unit ={
      val rnd= new Random()
      val procsToDisable = rnd.nextInt((nodeIdToActor.size-2)/2)
      disabledProcs += (round -> rnd.shuffle(nodeIdToActor.values.map(c=> c.path.name)).take(procsToDisable).toSet)
  }
}
