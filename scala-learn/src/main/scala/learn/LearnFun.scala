package learn

import java.util.Date

/**
  * @author junzhongliu
  * @date 2019/4/20 20:47
  */
object LearnFun {

  def main(args: Array[String]): Unit = {

    //定义普通函数
//    def max(x:Int,y:Int):Int = {
//      if(x>y){
//        return  x;
//      }else{
//        return y;
//      }
//    }
//    println(max(100,2));

    //单行定义普通函数
//    def max(x:Int,y:Int) = if(x>y) x else y;
//    println(max(2,100))

    //递归函数
//    def fun(x:Int):Int = {
//      if(x==1){
//        return x;
//      }else{
//        return x*fun(x-1);
//      }
//    }
//
//    print(fun(5));

    //带默认形参的参数
//    def fun(x:Int=100,y:Int=20) = {
//      x+y;
//    }
//    print(fun(y = 50));

    //参数长度可变的函数
//    def fun(s:String*) = {
//      s.foreach((element:String) => {
//        println(element);
//      })
//    }
//
//    fun("1","2","3","5","2");

    //匿名函数
//    var fun:(Int,Int) => Int = (x:Int,y:Int) =>{
//      x+y;
//    }
//
//    println(fun(1,2));

//    def showLog(d:Date,log:String): Unit ={
//      println("date is "+d+",log is "+log);
//    }
//
//    var date = new Date();
//    showLog(date,"a");
//    showLog(date,"b");
//    var fun = showLog(date,_:String);
//    fun("aaa");
//    fun("bbb");


    //高阶函数
    //函数的参数是函数
//    def fun(f:(Int,Int)=>Int,s:String): String = {
//      var i = f(10,20);
//      return i+"-"+s;
//    }
//
//    var result = fun((a:Int,b:Int) => {a*b},"hello");
//    println(result);


    //函数的返回类型是函数
    def fun(a:Int,b:Int):(String,String)=>String ={

      var value = a*b;

      def returnFun(s1:String,s2:String):String = {
        return s1+" hello"+s2+" world "+value;
      }
      returnFun;
    }

    println(fun(10,20)("s1","s2"));
  }



}
