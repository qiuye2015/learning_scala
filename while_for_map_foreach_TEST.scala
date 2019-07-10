import Array._
object LoopTest {

  def main(args: Array[String]): Unit = {
    var size = 10000;
    while(size<100000000){
    var arr = range(0,size)
        printTime(whileTest(arr))
        printTime(forTest(arr))
        printTime(foreachTest(arr))
        printTime(mapTest(arr))
        size *=10
        println() 
    }
  }

  def printTime(call : => Unit):Unit = {
    val startTime = System.currentTimeMillis()
    call
    println()
    println(s"use time : ${System.currentTimeMillis() - startTime}")
  }

  def whileTest(arr: Array[Int]):Unit = {
      var s = 0
      var j=0
      while ( j < arr.size){
        if(s %2==0) s +=arr(j)
        else s +=1
        j +=1
      }
      print(s"while s:$s")
  }

  def forTest(arr: Array[Int]):Unit = {
    var s = 0
    for ( j <- arr){
        if(s%2==0)s +=j
        else s +=1
    }
    print(s"for s:$s")
  }
  
  def foreachTest(arr: Array[Int]):Unit ={
    var s:Int= 0;
    arr.foreach(x => {
        if(s%2==0) s +=x
        else    s+=1
    })
    print(s"foreach s:$s")
  }
  def mapTest(arr: Array[Int]): Unit = {
    var s:Int = 0;
    arr.map(x=>{
        if(s%2==0)  s +=x
        else s +=1
    })
    print(s"map s:$s")
  }

}
