package learn

/**
  * @author junzhongliu
  * @date 2019/4/21 15:29
  */
object Learn_Match {
  def main(args: Array[String]): Unit = {
    var tuple = (1,"hello",3.0,true);
    val iterator = tuple.productIterator;
    while (iterator.hasNext){
      var v = iterator.next();
      matchTest(v);
    }

    def matchTest(value: Any)= {
      value match {
        case 1 => println("value is 1");
        case i:Int => println("zhengxing");
        case s:String => println("type is String");
        case 3.0 => println("value is 3");
        case _ => println("默认值");
      }
    }
  }

}
