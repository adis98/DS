package archipelago

import akka.actor.{ActorRef, ActorSystem, Props}

/*
class max_register extends Actor{
  var register : Set[(Int,Int)] = Set((0,-1))
  def receive: PartialFunction[Any,Unit] = {
    case (c:Int,v:Int) => //if some processor writes to the max register
      register = register + ((c,v)) //add element to set and send max value back
      sender() ! (register.max,"R_step_done")
  }
}

class adopt_commit_max extends Actor{
  var A = new ArrayBuffer[Int]()
  var B = new ArrayBuffer[(String,Int)]() //B contains a tuple of a string and an int for control and value
  for(i <- 0 until Main.N){
    A += -1 //initialize the array A
    B += (("",-1))
  }
  def receive: PartialFunction[Any, Unit] = {
    case ("propose_A_step",v:Int) =>  //make the adopt commit max object perform until the collect of A step
      val index = sender().path.name.toInt //gets the index of the register to write to for the sending processor
      A(index) = v
      val Sa = collectA(A)
      sender() ! (Sa,"A_step_done")

    case ("propose_B_step",sa:Set[Int]) =>
      val index = sender().path.name.toInt
      val Sa_temp = sa - (-1)
      if(Sa_temp.toList.length == 1){
        B(index) = ("commit",Sa_temp.toList.head)
      }
      else{
        B(index) = ("adopt",Sa_temp.max)
      }
      val Sb = collectB(B)
      sender() ! (Sb,"B_step_done")

  }
  def collectA(buff: ArrayBuffer[Int]): Set[Int] = {
    var Sx : Set[Int] = Set()
    for(i <- 0 until Main.N){
      Sx = Sx + buff(i)
    }
    Sx
  }
  def collectB(buff: ArrayBuffer[(String,Int)]): Set[(String,Int)] = {
    var Sx : Set[(String,Int)] = Set()
    for(i <- 0 until Main.N){
      Sx = Sx + buff(i)
    }
    Sx
  }
}

class Processor extends Actor{
  var state = 0 //this can be 0,1,2,3,4 to indicate None,R-done,A-done,B-done, finished
  var Sa : Set[Int] = Set()
  var Sb : Set[(String,Int)] = Set()
  var pValue:Int = -1 //this is the value to propose. Initially -1
  var c = 0 //index of adopt_commit_max_object to refer to
  def receive: PartialFunction[Any, Unit] = {
    case ((c_prime:Int,v:Int),"R_step_done") =>
      state = 1
      //println(self.path.name,"R_step done","state:",state)
      c = c_prime
      pValue = v
      Main.puppeteer ! "round_done"

    case (sa:Set[Int],"A_step_done") =>
      state = 2
      //println(self.path.name,"A_step done","state:",state)
      Sa = sa
      Main.puppeteer ! "round_done"

    case (sb:Set[(String,Int)],"B_step_done") =>
      state = 3
      Sb = sb
      val Sb_temp = Sb - (("",-1))
      //println("Sb for", self.path.name,"sb:",Sb_temp)
      if(Sb_temp.toList.length == 1){
        val (control,v) = Sb_temp.toList.head
        c = c + 1
        if(control == "commit")
          state = 4
        else
          state = 0
        pValue = v

      }
      else if(Sb_temp.toList.length != 1){
        c = c + 1
        breakable{
          for(i <- Sb_temp.toList.indices){
            val (ctrl,vl) = Sb_temp.toList(i)
            if(ctrl == "commit"){
              pValue = vl
              state = 0
              break
            }
          }
        }
        if(state != 0){
          val (ctrl,vl) = Sb.max
          pValue = vl
          state = 0
        }
      }
      //println(self.path.name,"B_step done","state:",state)
      Main.puppeteer ! "round_done"

    case "enabled" => //enabled for this round by puppeteer
      if(pValue == -1){
        pValue = self.path.name.toInt //this can be changed to a more generic value later
      }
      if(state == 0){
        println("node: "+self.path.name+" starting R step")
        R_step(pValue,c)
      }
      else if(state == 1){
        println("node: "+self.path.name+" starting A step")
        A_step(c,pValue)
      }
      else if(state == 2){
        println("node: "+self.path.name+" starting B step")
        B_step(c,Sa)
      }
      else if(state == 4){ //finished process
        Main.puppeteer ! "already_decided"
        Main.puppeteer ! "round_done"
      }

    case "disabled" =>
      if(state == 4){
        Main.puppeteer ! "already_decided"
      }
      else{
        println("node: "+self.path.name+" disabled")
      }
      Main.puppeteer ! "round_done"

    case "printFinalValue" =>
      println("node: "+self.path.name + " value: "+pValue.toString)

  }
  def R_step(pValue:Int,c:Int): Unit ={
    Main.m ! (c,pValue)
  }
  def A_step(c:Int,v:Int): Unit = {
    Main.C(c) ! ("propose_A_step",v)
  }
  def B_step(c:Int,Sa:Set[Int]): Unit ={
    Main.C(c) ! ("propose_B_step",Sa)
  }
}

class Puppeteer extends Actor {
  var responses = 0
  var roundCnt = 0
  var procStats = new ArrayBuffer[Int]() //binary array that holds 0/1 for disabled/enabled processors in current round
  var alreadyDecided = 0
  def receive: PartialFunction[Any, Unit] = {
    case "beginSimulation" =>
      beginSimulation()

    case "round_done" =>
      responses = (responses + 1) % Main.N
      if (responses == 0) {
        roundCnt = roundCnt + 1
        println("----------------round done------------------")
        if(roundCnt == Main.rounds){
          for (i <- 0 until Main.N) {
            Main.nodes(i) ! "printFinalValue"
          }
        }
        else if(alreadyDecided != Main.N){
          procStats = getProcStatus(roundCnt)
          self ! ("new_round",procStats)
        }
        else if(alreadyDecided == Main.N){
          for (i <- 0 until Main.N) {
            Main.nodes(i) ! "printFinalValue"
          }
        }
      }

    case ("new_round",procStats:ArrayBuffer[Int]) =>
      alreadyDecided = 0
      println("---------------------round "+roundCnt.toString+"----------------")
      for (i <- 0 until Main.N) {
        if(procStats(i) == 1)
          Main.nodes(i) ! "enabled"
        else{
          Main.nodes(i) ! "disabled"
        }
      }

    case "already_decided" =>
      alreadyDecided += 1
      if(alreadyDecided == Main.N){
        println("looks like all have converged already....stopping simulation early")
      }

  }
  def beginSimulation(): Unit = {
    println("beginning simulation")
    if(!Main.adversaryMode) {
      println("--------------STANDARD MODE---------------")
      procStats = getProcStatus(roundCnt) //this returns an array of all processes to disabled/enable for the current round
      self ! ("new_round",procStats)
    }
    else if(Main.adversaryMode && Main.N != 2 && Main.K != 1){
      println("PLEASE SET THE NUMBER OF PROCESSORS TO 2 AND DISABLED PROCESSOR LIMIT TO 1 FOR ADVERSARY MODE")
    }
    else{
      println("--------------ADVERSARY MODE----------------")
      procStats = getProcStatus(roundCnt) //this returns an array of all processes to disabled/enable for the current round
      self ! ("new_round",procStats)
    }

  }
  def getProcStatus(rnd: Int): ArrayBuffer[Int] = {
    if(!Main.adversaryMode){
      val nodeList = List.range(0,Main.N)
      val shuffledList = Random.shuffle(nodeList)
      val disabledNodes = shuffledList.take(Main.K)
      val finalStat = new ArrayBuffer[Int]()
      for(i <-0 until Main.N){
        if(disabledNodes.contains(i)){
          finalStat += 0
        }
        else{
          finalStat += 1
        }
      }
      finalStat
    }
    else{
      val finalStat = new ArrayBuffer[Int]()
      if(rnd%5 == 0 ){
        finalStat += 1
        finalStat += 0
      }
      else if(rnd%5 == 1){
        finalStat += 1
        finalStat += 1
      }
      else if(rnd%5 == 2){
        finalStat += 0
        finalStat += 1
      }
      else if(rnd%5 == 3){
        finalStat += 0
        finalStat += 1
      }
      else if(rnd%5 == 4){
        finalStat += 1
        finalStat += 0
      }
      finalStat
    }
  }
}
*/
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