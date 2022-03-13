package oft_archipelago

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import oft_archipelago.Node.{AResponse, BResponse, Command, RResponse}

object Node {

  sealed trait Command
  final case class RBroadcast(i: Int, v: Int, replyTo: ActorRef[Node.Command]) extends Command
  private final case class RResponse(i: Int, R: Set[(Int, Int)]) extends Command
  final case class ABroadcast(i: Int, v: Int, replyTo: ActorRef[Node.Command]) extends Command
  private final case class AResponse(i: Int, aJ: Set[Int]) extends Command
  final case class BBroadcast(i: Int, flag: Boolean, v: Int, replyTo: ActorRef[Node.Command]) extends Command
  private final case class BResponse(i: Int, bJ: Set[(Boolean, Int)]) extends Command

  def apply(nodeId: String): Behavior[Command] =
    Behaviors.setup(context => new Node(context, nodeId, 0, Set.empty, List.empty, List.empty, List.empty, List.empty, List.empty))

}

class Node(context: ActorContext[Node.Command], nodeId: String, i: Int, R: Set[(Int, Int)], A: List[Set[Int]], B: List[Set[(Boolean, Int)]], RResponses: List[RResponse], AResponses: List[AResponse], BResponses: List[BResponse]) extends AbstractBehavior[Command](context) {
  context.log.info("New node created/updated: {}", nodeId)
  import Node._
  override def onMessage(msg: Command): Behavior[Command] =
    msg match {
      case _ @ RBroadcast(j, v, replyTo) =>
        val newR = R + ((j, v))
        replyTo ! RResponse(j, newR)
        new Node(context, nodeId, i, newR, A, B, RResponses, AResponses, BResponses)

      case _ @ ABroadcast(j, v, replyTo) =>
        val newA: List[Set[Int]]  = if(A.size <= j) {
          var tempA = A
          while(tempA.size <= j)
            tempA = tempA :+ Set.empty
          tempA.updated(j, Set(v))
        }  else
          A.updated(j, A(j) + v)
        replyTo ! AResponse(j, newA(j))
        new Node(context, nodeId, i, R, newA, B, RResponses, AResponses, BResponses)

      case _ @ BBroadcast(j, flag, v, replyTo) =>
        val newB: List[Set[(Boolean, Int)]]  = if(B.size <= j) {
          var tempB = B
          while(tempB.size <= j)
            tempB = tempB :+ Set.empty
          tempB.updated(j, Set((flag, v)))
        }  else
          B.updated(j, B(j) + (flag, v))
        replyTo ! BResponse(j, newB(j))
        new Node(context, nodeId, i, R, A, newB, RResponses, AResponses, BResponses)

      case resp @ RResponse(j, rcvdR) =>
        val newRResponses = RResponses :+ resp
        if(newRResponses.size > ((Main.N + 1) / 2).floor) {
          var newR = R
          for (response <- newRResponses) {
            newR = newR + response.R
          }
          var maxI = 0
          for (tuple <- newR) {
            if(tuple._1 > maxI)
              maxI = tuple._1
          }
          var maxV = 0
          for (tuple <- newR) {
            if(tuple._1 == maxI && tuple._2 > maxV)
              maxV = tuple._2
          }
          //broadcastA
          new Node(context, nodeId, maxI, newR, A, B, List.empty, AResponses, BResponses)
        }

      case resp @ AResponse(j, rcvdAJ) =>
        val newAResponses = AResponses :+ resp
        if(newAResponses.size > ((Main.N + 1) / 2).floor) {
          var S = Set.empty
          for (response <- newAResponses) {
            S = S + response.aJ
          }
          if(S.size == 1) {
            //broadcastB - true, val
          } else {
            //broadcastB - false, max S
          }
          new Node(context, nodeId, i, R, A, B, RResponses, List.empty, BResponses)
        }

      case resp @ BResponse(j, rcvdBJ) =>
        val newBResponses = BResponses :+ resp
        if(newBResponses.size > ((Main.N + 1) / 2).floor) {
          var S: Set[(Boolean, Int)] = Set.empty
          for (response <- newBResponses) {
            S = S + response.bJ
          }
          if(S.size == 1) {
            if(S.head._1) {
              //broadcast commit val to group
            }
          } else {
            val trueElement = S.filter(x => x._1)
            if(trueElement.size == 1) {
              //broadcast adopt val to group
            } else {
              //broadcast adopt max val to group
            }
          }
          new Node(context, nodeId, i, R, A, B, RResponses, List.empty, BResponses)
        }
      case default =>
        this
    }
}
