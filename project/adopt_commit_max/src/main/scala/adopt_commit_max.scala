import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

//TODO: Make the adopt commit max object

class Processor extends Actor{    // Extending actor trait
  def receive = {                 //  Receiving message
    case "enable" => {
      println("execution for node "+ self.path.name)
      //TODO: For now it just prints, but here the nodes would actually run archipelago
      sender() ! 1
    }
    case "disable" =>
      println("disabled node "+self.path.name)      // Disabled case
      sender() ! 1
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