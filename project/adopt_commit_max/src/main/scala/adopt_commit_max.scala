import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}
import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex

//TODO: Make the adopt commit max object
class adopt_commit_max extends Actor{
  var A = new ArrayBuffer[Int]()
  var B = new ArrayBuffer[String]()
  for(i <- 0 until Main.N){
    A += -1 //initial value indication empty
    B += "" //empty string initially
  }
  var index: Int = -1
  def receive: PartialFunction[Any,Unit] = {
    case (v:Int,ind:Int) => {
      //println("received",v)
      //println(sender().path.name)
      index = ind
      println(B)
      //print("indexed")
      propose(v)
    }
  }
  def propose(v: Int): Unit ={
    A(index) = v
    var Sa : Set[Int] = Set()
    for(i <- 0 until Main.N){
      Sa = Sa + A(i)
    }
    val Sa_temp = Sa - (-1)
    if(Sa_temp.toList.length == 1){
      B(index) = "commit_"+Sa_temp.toList(0).toString
    }
    else{
      B(index) = "adopt_"+Sa.max.toString
    }
    var Sb : Set[String] = Set()
    for(i <- 0 until Main.N){
      Sb  = Sb + B(i)
    }
    //println(Sb)

    val commitPattern: Regex = "commit".r
    val Sb_temp = Sb - ""
    if(Sb_temp.toList.length == 1){
      //println("ad_max_"+Sb_temp.toList(0))
      //println("replied")
      sender() ! "ad_max_"+Sb_temp.toList(0)
    }
    else{
      var done = false
      breakable{
        for(i <- 0 until Sb_temp.toList.length){
          commitPattern.findFirstMatchIn(Sb_temp.toList(i)) match{
            case Some(_) => {
              //println("replied")
              sender() ! "ad_max_"+Sb_temp.toList(i)
              done = true
              break
            }
            case None =>
          }
        }
        if(done == false){
          //println("replied")
          sender() ! "ad_max_"+Sb.max
        }
      }
    }

  }
}

class Processor extends Actor{    // Extending actor trait
  var value = self.path.name.toInt //can be changed
  var index = self.path.name.toInt
  def receive = {                 //  Receiving message
    case "enable" => {
      println("execution for node "+ self.path.name)
      //TODO: For now it just prints, but here the nodes would actually run archipelago
      implicit val timeout: Timeout = Timeout(100.seconds)
      //ask(Main.node(i) , "disable").mapTo[Int]
      val res = (Main.ad_max ? (value,index))
      Await.result(res,Duration.Inf)
      res.onComplete{
        case Success(resp) => println("got: "+resp)
        case f @ Failure(exception) =>
          exception.printStackTrace()
          f
      }
      sender() ! 1

    }
    case "disable" =>{
      println("disabled node "+self.path.name)      // Disabled case
      sender() ! 1
    }
  }
}

class Puppeteer extends Actor{
  implicit val timeout: Timeout = Timeout(100.seconds)
  def receive: PartialFunction[Any,Unit] = {
    case "BeginSimulation" => {
      println("Right, I'll begin the simulation")
      beginSimulation()
    }
  }
  def beginSimulation(): Unit ={
    for(rnd <- 1 to Main.rounds){
      println("Beginning round "+ rnd.toString)
      val nodeList = List.range(0,Main.N)
      val shuffledList = Random.shuffle(nodeList)
      val disabledNodes = shuffledList.take(Main.K) //take the first K nodes to be the disabled ones
      val results = Array.ofDim[Future[Int]](Main.N) //expect responses from the enabled nodes alone on completion
      for(i <- 0 until Main.N){
        if(disabledNodes contains(i)){
          results(i) = ask(Main.node(i) , "disable").mapTo[Int] //disable the node for this round
        }
        else{
          results(i) = ask(Main.node(i) , "enable").mapTo[Int]
        }
      }
      val aggList = results.toList
      val futList = Future.sequence(aggList)
      Await.result(futList,Duration.Inf)
      println("----------round done------------")
    }
  }
}

object Main{
  val rounds = 2 //no. of rounds
  val K = 1 //the max number of disabled processes
  val N = 5 //the total number of nodes
  val node = new Array[ActorRef](N)
  val actorSystem = ActorSystem("ActorSystem");
  val puppeteer = actorSystem.actorOf(Props[Puppeteer])
  val ad_max = actorSystem.actorOf(Props[adopt_commit_max])
  def main(args:Array[String]){
    for(i <- 0 until N){
      node(i) = actorSystem.actorOf(Props[Processor],i.toString)
    }
    puppeteer ! "BeginSimulation"
    //var actor = actorSystem.actorOf(Props[HelloAkka],"HelloAkka")        //Creating actor
    //actor ! "Hello Akka"                                                // Sending messages by using !
    //actor ! 100.52
  }
}