val str = "x1 = x_1 and x2=x_2"
val strList = str.split("and")
val str2 = str.split("and").map{
  case s =>
    val sAlias = s.split("=")
    "p1."+sAlias(0).trim+" = "+"p2."+sAlias(1).trim
}.mkString(" and ")


val a = Seq("aa","cc")
val b = Seq("aaa","bb")

a++b

val c = Seq("aa","bb",199).toArray

c :+ 1000

1000 +: c
