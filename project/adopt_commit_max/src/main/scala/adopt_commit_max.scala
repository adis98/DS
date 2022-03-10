import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Random, Success}

//TODO: Make the adopt commit max object
class adopt_commit_max extends Actor{
  var A = new ArrayBuffer[Int]()
  var B = new ArrayBuffer[(String,Int)]()
  for(i <- 0 until Main.N){
    A += -1 //initial value indication empty
    B += (("",-1)) //empty string initially
  }
  var index: Int = -1
  def receive: PartialFunction[Any,Unit] = {
    case (v:Int,ind:Int) => {
      //println("received",v)
      //println(sender().path.name)
      index = ind
      //println(B)
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
      B(index) = ("commit",Sa_temp.toList(0))
    }
    else{
      B(index) = ("adopt",Sa.max)
    }
    var Sb : Set[(String,Int)] = Set()
    for(i <- 0 until Main.N){
      Sb  = Sb + B(i)
    }
    //println(Sb)
    val Sb_temp = (Sb - (("",-1))).toList

    if(Sb_temp.length == 1){
      //println("ad_max_"+Sb_temp.toList(0))
      //println("replied")
      sender() ! Sb_temp(0) //send the pair (control, value)
    }
    else{
      var done = false
      breakable{
        //for(i <-0 until List(Sb_temp.length)
        for(i <- 0 until Sb_temp.length){
          if(Sb_temp(i) == ("commit", Int)){
            sender() ! Sb_temp(i)
            done = true
            break
          }
          //done = true
          //break
        }
        if(done == false){
          //println("replied")
          sender() ! maximum(Sb_temp)
        }
      }
    }
  }

  def maximum(Sb_temp: List[(String, Int)]): (String, Int) = {
    var (maxControl,maxVal) = Sb_temp(0)
    for (i <- 1 until Sb_temp.length) {
      var (currControl,currVal) = Sb_temp(i)
      if(currControl == maxControl && currVal > maxVal){
        maxVal = currVal
      }
      else if(currControl == "commit" && maxControl == "adopt"){
        maxControl = currControl
        maxVal = currVal
      }
    }
    (maxControl,maxVal)
  }
}

class Processor extends Actor{    // Extending actor trait
  var valu = Random.nextInt(10) //can be changed
  var index = self.path.name.toInt
  def receive = {                 //  Receiving message
    case ("enable",adp_index:Int) => {
      println("execution for node "+ self.path.name)
      //TODO: For now it just prints, but here the nodes would actually run archipelago
      implicit val timeout: Timeout = Timeout(100.seconds)
      //archipelago(value, personal_index,register_index)
      val res = archipelago(valu, index,adp_index)
      Await.result(res,Duration.Inf)
      res.onComplete{
        case Success((control,vals)) => {
          println("got: "+(control,vals))
          if(control == "adopt"){
            valu = vals.asInstanceOf[Int]
            //println("value is ",valu)
          }
          else if(control == "commit"){
            valu = vals.asInstanceOf[Int]
          }
        }
        case f @ Failure(exception) =>
          exception.printStackTrace()
          f
      }
      sender() ! 1
    }
    case ("disable",adp_index) =>{
      println("disabled node "+self.path.name)      // Disabled case
      sender() ! 1
    }
    case "give_final" => {
      sender() ! ("final",valu)
    }
  }

  def archipelago(value:Int, personal_index:Int,reg_index:Int): Future[Any] = {
    implicit val timeout: Timeout = Timeout(100.seconds)
    val res  = Main.shared_array_max(reg_index) ? (value,personal_index)
    Await.result(res,Duration.Inf)
    res.onComplete{
      case Success((control,vals)) => println("got: "+(control,vals))
      case f @ Failure(exception) =>
        exception.printStackTrace()
        f
    }
    res
  }

}

class Puppeteer extends Actor{
  implicit val timeout: Timeout = Timeout(100.seconds)
  def receive: PartialFunction[Any,Unit] = {
    case "BeginSimulation" => {
      println("Right, I'll begin the simulation")
      beginSimulation()
    }
    case ("final",value:Int) => {
      println("final value for processor "+sender().path.name+" = "+value.toString)
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
          results(i) = ask(Main.node(i) , ("disable",rnd-1)).mapTo[Int] //disable the node for this round
        }
        else{
          results(i) = ask(Main.node(i) , ("enable",rnd-1)).mapTo[Int]
        }
      }
      val aggList = results.toList
      val futList = Future.sequence(aggList)
      Await.result(futList,Duration.Inf)
      println("----------round done------------")
    }
    for(i <- 0 until Main.N){
      Main.node(i) ! "give_final"
    }
  }
}

object Main{
  val rounds = 3 //no. of rounds
  val shared_commit_array_size = rounds
  val shared_array_max = new Array[ActorRef](shared_commit_array_size) //the array of adopt commit max objects
  val K = 15 //the max number of disabled processes
  val N = 20 //the total number of nodes
  val node = new Array[ActorRef](N)
  val actorSystem = ActorSystem("ActorSystem");
  val puppeteer = actorSystem.actorOf(Props[Puppeteer])
  val ad_max = actorSystem.actorOf(Props[adopt_commit_max])
  def main(args:Array[String]){
    for(i <- 0 until N){
      node(i) = actorSystem.actorOf(Props[Processor],i.toString)
    }
    for(i <- 0 until shared_commit_array_size){
      shared_array_max(i) = actorSystem.actorOf(Props[adopt_commit_max],"obj_"+i.toString)
    }
    puppeteer ! "BeginSimulation"

  }
}