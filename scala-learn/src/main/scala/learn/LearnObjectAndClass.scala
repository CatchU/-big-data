package learn

/**
  * @author junzhongliu
  */
object LearnObjectAndClass {

  def main(args: Array[String]): Unit = {

//    val a = 500;
//    if(a<100){
//      println("a小于100");
//    }else if(a<500){
//      println("a大于100小于500");
//    }else{
//      println("a大于500");
//    }


//    println(1 to 10);
//    println(1.to(10));
//    println(1 until(10));
//    println(1.until(10));
//    println(1.to(10,3));
//    println(1.until(10,3));


//    for(i <- 1 to 10; j <- 1 until 10){
//      println("i="+i+" j="+j);
//    }

//    for(i <- 1 to 100 ; if(i%2==0)){
//      println(i);
//    }


//    for(i <- 1 to 10;j <- 1 to 9){
//      if(i>=j){
//        print(i + "*" + j + "=" + i*j + "\t");
//      }
//      if(i==j){
//        println();
//      }
//    }

    var person = new Person("123",18);
    println(person.age);
    
    person.show();

  }

}


