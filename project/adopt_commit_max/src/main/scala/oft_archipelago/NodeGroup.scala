package oft_archipelago

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, LoggerOps}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}
import oft_archipelago.Main.NodeRegistered

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ListBuffer
import scala.util.Random

object NodeGroup {
  def apply(): Behavior[Command] =
    Behaviors.setup(context => new NodeGroup(context))

  sealed trait Command

  final case class RequestTrackDevice(nodeId: String, n: Int, replyTo: ActorRef[Main.Command]) extends Command

  final case class Start(f: Double) extends Command

  final case class Commit(value: Int, nodeId: String, i: Int) extends Command

  final case class BroadcastR(rBroadcast: Node.RBroadcast) extends Command

  final case class BroadcastA(aBroadcast: Node.ABroadcast) extends Command

  final case class BroadcastB(bBroadcast: Node.BBroadcast) extends Command
}

class NodeGroup(context: ActorContext[NodeGroup.Command])
  extends AbstractBehavior[NodeGroup.Command](context) {

  import NodeGroup._

  private var nodeIdToActor = Map.empty[String, ActorRef[Node.Command]]
  private var nodeIdToCommit = Map.empty[String, Boolean]
  private var disabledProcs = Map.empty[Int, Set[String]]
  private var blockedABroadcasts = ListBuffer.empty[BroadcastA]
  private var blockedRBroadcasts = ListBuffer.empty[BroadcastR]
  private var blockedBBroadcasts = ListBuffer.empty[BroadcastB]
  private var blockedAMailbox = Map.empty[ActorRef[Node.Command], ListBuffer[Node.ABroadcast]]
  private var blockedBMailbox = Map.empty[ActorRef[Node.Command], ListBuffer[Node.BBroadcast]]
  private var blockedRMailbox = Map.empty[ActorRef[Node.Command], ListBuffer[Node.RBroadcast]]
  private var blockedProcesses: Double = 0
  private var maxRounds = 0

//  context.log.info("NodeGroup started")

  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case trackMsg@RequestTrackDevice(nodeId, n, replyTo) =>
        nodeIdToActor.get(nodeId) match {
          case Some(nodeActor) =>
            replyTo ! NodeRegistered(nodeActor)
          case None =>
//            context.log.info("Creating node actor for {}", trackMsg.nodeId)
            val nodeActor = context.spawn(Node(n, nodeId), s"node-$nodeId")
            nodeIdToCommit += nodeId -> false
            nodeIdToActor += nodeId -> nodeActor
            blockedRMailbox += nodeActor -> ListBuffer.empty[Node.RBroadcast]
            blockedAMailbox += nodeActor -> ListBuffer.empty[Node.ABroadcast]
            blockedBMailbox += nodeActor -> ListBuffer.empty[Node.BBroadcast]
//            context.log.info("Map Size {}", nodeIdToActor.size)
            if (nodeIdToActor.size > n - 1) {
              nodeIdToActor.foreach { case (_, nodeActor) =>
                val value = Random.nextInt(n)
                nodeActor ! Node.Start(value)
              }
            }
            replyTo ! NodeRegistered(nodeActor)
        }
        this
      case msg@BroadcastR(rBroadcast) =>
        if (!disabledProcs.contains(rBroadcast.i)) {
          blockNewProcesses(rBroadcast.i)
          blockedRBroadcasts.foreach { case (broadcastR: BroadcastR) =>
            nodeIdToActor.foreach { case (_, nodeActor) =>
              nodeActor ! broadcastR.rBroadcast
            }
          }
          blockedRMailbox.foreach { case (actorRef, listBuffer) =>
            if (listBuffer.nonEmpty) {
              listBuffer.foreach { case (message) =>
                actorRef ! message
              }
              blockedRMailbox += actorRef -> ListBuffer.empty
            }
          }
          blockedRBroadcasts = ListBuffer.empty[BroadcastR]
        }
        val procsBlockedInRound = disabledProcs.get(rBroadcast.i)
        if (!procsBlockedInRound.get.contains(rBroadcast.replyTo.path.name)) {
          nodeIdToActor.foreach { case (_, nodeActor) =>
            if (nodeActor != rBroadcast.replyTo)
              if (!procsBlockedInRound.get.contains(nodeActor.path.name)) {
                nodeActor ! rBroadcast
              } else {
//                context.log.info("Process {} was blocked from receiving R-step message", nodeActor)
                var nodeActorBlockedMailbox = blockedRMailbox.get(nodeActor).get
                nodeActorBlockedMailbox += rBroadcast
                blockedRMailbox += nodeActor -> nodeActorBlockedMailbox
              }
          }
        } else {
//          context.log.info("Process {} was blocked from sending R-step message", rBroadcast.replyTo)
          blockedRBroadcasts += msg
        }
        this
      case msg@BroadcastA(aBroadcast) =>
        if (!disabledProcs.contains(aBroadcast.i)) {
          blockNewProcesses(aBroadcast.i)
          blockedABroadcasts.foreach { case (broadcastA: BroadcastA) =>
            nodeIdToActor.foreach { case (_, nodeActor) =>
              nodeActor ! broadcastA.aBroadcast
            }
          }
          blockedAMailbox.foreach { case (actorRef, listBuffer) =>
            if (listBuffer.nonEmpty) {
              listBuffer.foreach { case (message) =>
                actorRef ! message
              }
            }
            blockedAMailbox += actorRef -> ListBuffer.empty
          }
          blockedABroadcasts = ListBuffer.empty[BroadcastA]
        }
        nodeIdToActor.foreach { case (_, nodeActor) =>
          if (nodeActor != aBroadcast.replyTo)
            nodeActor ! aBroadcast
        }
        this
      case msg
        @BroadcastB(bBroadcast)
      =>
        if (!disabledProcs.contains(bBroadcast.i)) {
          blockNewProcesses(bBroadcast.i)
          blockedBBroadcasts.foreach { case (broadcastB: BroadcastB) =>
            nodeIdToActor.foreach { case (_, nodeActor) =>
              nodeActor ! broadcastB.bBroadcast
            }
          }
          blockedBMailbox.foreach { case (actorRef, listBuffer) =>
            if (listBuffer.nonEmpty) {
              listBuffer.foreach { case (message) =>
                actorRef ! message
              }
            }
            blockedBMailbox += actorRef -> ListBuffer.empty
          }
          blockedBBroadcasts = ListBuffer.empty[BroadcastB]
        }
        nodeIdToActor.foreach { case (_, nodeActor) =>
          if (nodeActor != bBroadcast.replyTo)
            nodeActor ! bBroadcast
        }
        this
      case _
        @Start(f: Double)
      =>
        blockedProcesses=f
//        context.log.info("Starting Archipelago")
        this
      case _
        @Commit(value, nodeId, i)
      =>
//        context.log.info("Received commit " + value.toString + " from " + nodeId + " in round " + i.toString)
        nodeIdToCommit += nodeId -> true
        val nonCommitedNodes = nodeIdToCommit.filter(!_._2)
//        context.log.info("values committed " + nonCommitedNodes.size.toString)
        maxRounds = math.max(maxRounds, i+1)
//        val procsToDisable = (nodeIdToActor.size * blockedProcesses).floor.toInt
        if(nonCommitedNodes.size==0) {
//          context.log.info("Converged in " + maxRounds.toString + " rounds")
          logResults(maxRounds)
          return Behaviors.empty
        }
        if(maxRounds>1000)
          return Behaviors.empty
        this
    }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case PostStop =>
//      context.log.info("NodeGroup stopped")
      this
  }

  def blockNewProcesses(round: Int): Unit = {
    val rnd = new Random()
    val procsToDisable = (nodeIdToActor.size * blockedProcesses).floor.toInt
    disabledProcs += (round -> rnd.shuffle(nodeIdToActor.values.map(c => c.path.name)).take(procsToDisable).toSet)
  }

  def logResults(rounds: Int): Unit ={
    val bw = new BufferedWriter(new FileWriter(new File("file.txt"), true))
    val stringToWrite = this.nodeIdToActor.size.toString +" "+this.blockedProcesses.toString+" "+maxRounds.toString + "\n"
    bw.write(stringToWrite)
    bw.close
  }
}
