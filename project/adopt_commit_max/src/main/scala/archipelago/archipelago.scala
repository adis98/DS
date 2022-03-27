package archipelago

import akka.actor.{ActorRef, ActorSystem, Props}


object Main{
  val K = 1 //the max number of disabled processes per-round
  val rounds = 10 //no. of rounds
  val adversaryMode = false //whether the adversary selection strategy is to be used
  //TODO: Currently adversaryMode only works for 2 processors
  val N = 7 //the number of processors
  val actorSystem = ActorSystem("ActorSystem")
  val m = actorSystem.actorOf(Props[max_register])
  val puppeteer = actorSystem.actorOf(Props[Puppeteer])
  val C = new Array[ActorRef](rounds) //array of adopt commit max objects
  val nodes = new Array[ActorRef](N)
  for(i <- 0 until rounds){
    C(i) = actorSystem.actorOf(Props[adopt_commit_max])
  }
  for(i <- 0 until N){
    nodes(i) = actorSystem.actorOf(Props[Processor],i.toString)
  }
  def main(args:Array[String]){
    puppeteer ! "beginSimulation"
  }
}