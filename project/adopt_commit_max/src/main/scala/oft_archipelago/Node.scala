package oft_archipelago

import akka.actor.typed.scaladsl.adapter.TypedActorContextOps
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import oft_archipelago.Node.{AResponse, BResponse, Command, RResponse}
import oft_archipelago.NodeGroup.{BroadcastA, BroadcastB, BroadcastR, Commit}

object Node {

  sealed trait Command

  final case class RBroadcast(i: Int, v: Int, replyTo: ActorRef[Node.Command]) extends Command
  private final case class RResponse(i: Int, R: Set[(Int, Int)]) extends Command
  final case class ABroadcast(i: Int, v: Int, replyTo: ActorRef[Node.Command]) extends Command
  private final case class AResponse(i: Int, aJ: Set[Int]) extends Command
  final case class BBroadcast(i: Int, flag: Boolean, v: Int, replyTo: ActorRef[Node.Command]) extends Command
  private final case class BResponse(i: Int, bJ: Set[(Boolean, Int)]) extends Command
  final case class Start(v: Int) extends Command
  final case class Stop() extends Command

  def apply(n: Int, nodeId: String): Behavior[Command] =
    Behaviors.setup(context => new Node(context, n, nodeId, 0, Set.empty, List.empty, List.empty, List.empty, List.empty, List.empty))

}

class Node(context: ActorContext[Node.Command], n: Int, nodeId: String, i: Int, R: Set[(Int, Int)], A: List[Set[Int]], B: List[Set[(Boolean, Int)]], RResponses: List[RResponse], AResponses: List[AResponse], BResponses: List[BResponse]) extends AbstractBehavior[Command](context) {
  import Node._

  override def onMessage(msg: Command): Behavior[Command] = {
    msg match {
      case _@RBroadcast(j, v, replyTo) =>
        val newR = R + ((j, v))
        replyTo ! RResponse(j, newR)
        new Node(context, n, nodeId, i, newR, A, B, RResponses, AResponses, BResponses)

      case _@ABroadcast(j, v, replyTo) =>
        val newA: List[Set[Int]] = if (A.size <= j) {
          var tempA = A
          while (tempA.size <= j)
            tempA = tempA :+ Set.empty
          tempA.updated(j, Set(v))
        } else
          A.updated(j, A(j) + v)
        replyTo ! AResponse(j, newA(j))
        new Node(context, n, nodeId, i, R, newA, B, RResponses, AResponses, BResponses)

      case _@BBroadcast(j, flag, v, replyTo) =>
        val newB: List[Set[(Boolean, Int)]] = if (B.size <= j) {
          var tempB = B
          while (tempB.size <= j)
            tempB = tempB :+ Set.empty
          tempB.updated(j, Set((flag, v)))
        } else {
          val bJ = B(j)
          val updatedBj: Set[(Boolean, Int)] = bJ + ((flag, v))
          B.updated(j, updatedBj)
        }
        replyTo ! BResponse(j, newB(j))
        new Node(context, n, nodeId, i, R, A, newB, RResponses, AResponses, BResponses)

      case resp@RResponse(j, rcvdR) =>
        val newRResponses = RResponses :+ resp
        if (newRResponses.size > ((n + 1) / 2).floor) {
          var newR = R
          for (response <- newRResponses) {
            newR = newR union response.R
          }
          var maxI = 0
          for (tuple <- newR) {
            if (tuple._1 > maxI)
              maxI = tuple._1
          }
          var maxV = 0
          for (tuple <- newR) {
            if (tuple._1 == maxI && tuple._2 > maxV)
              maxV = tuple._2
          }
          val parentActorRef = context.toClassic.parent
          parentActorRef ! BroadcastA(ABroadcast(maxI, maxV, context.self))
          new Node(context, n, nodeId, maxI, newR, A, B, List.empty, AResponses, BResponses)
        } else {
          new Node(context, n, nodeId, i, R, A, B, newRResponses, AResponses, BResponses)
        }

      case resp@AResponse(j, rcvdAJ) =>
        val newAResponses = AResponses :+ resp
        if (newAResponses.size > ((n + 1) / 2).floor) {
          var S: Set[Int] = Set.empty
          for (response <- newAResponses) {
            S = S union response.aJ
          }
          val parentActorRef = context.toClassic.parent
          if (S.size == 1) {
            parentActorRef ! BroadcastB(BBroadcast(i, flag = true, S.head, context.self))
          } else {
            parentActorRef ! BroadcastB(BBroadcast(i, flag = false, S.max, context.self))
          }
          new Node(context, n, nodeId, i, R, A, B, RResponses, List.empty, BResponses)
        } else {
          new Node(context, n, nodeId, i, R, A, B, RResponses, newAResponses, BResponses)
        }

      case resp@BResponse(j, rcvdBJ) =>
        val newBResponses = BResponses :+ resp
        if (newBResponses.size > ((n + 1) / 2).floor) {
          var S: Set[(Boolean, Int)] = Set.empty
          for (response <- newBResponses) {
            S = S union response.bJ
          }
          val parentActorRef = context.toClassic.parent
          if (S.size == 1) {
            if (S.head._1) {
              parentActorRef ! Commit(S.head._2, nodeId, i)
              return this
            }
          } else {
            val trueElement = S.filter(x => x._1)
            if (trueElement.size == 1) {
//              context.log.info("Adopt! single elem " + trueElement.head._2 + " in " + nodeId + " " + (i+1).toString)
              propose(i + 1, trueElement.head._2)
            } else {
//              context.log.info("Adopt! multiple elem in " + nodeId + " " + (i+1).toString)
              propose(i + 1, S.maxBy(_._2)._2)
            }
          }
          new Node(context, n, nodeId, i + 1, R, A, B, RResponses, AResponses, List.empty)
        } else {
          new Node(context, n, nodeId, i, R, A, B, RResponses, AResponses, newBResponses)
        }

      case _@Start(v) =>
//        context.log.info("Started! in nodeId " + nodeId)
        propose(i, v)
        this
      case _@Stop() =>
        Behaviors.stopped
      case default =>
        this
    }
  }

  def propose(i: Int, v: Int): Unit = {
    val parentActorRef = context.toClassic.parent
    parentActorRef ! BroadcastR(RBroadcast(i, v, context.self))
  }
}
