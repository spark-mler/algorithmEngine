package com.burness.algorithm.demo

import com.burness.algorithm.preprocess.Join
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by burness on 16-6-21.
 */
object ExampleJoin {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Join example")
    val sc = new SparkContext(conf)

    val args_test = Seq("--leftTableName","join_left","--rightTableName","join_right","--outputTableName","join_output",
    "--joinMethod","INNER","--joinConditions","ds=ds and x1=x1","--leftOutFieldsName","x1,x_1\tx2,x_2\tds,ds",
    "--rightOutFieldsName","x1,x1\tx2,x2").toArray

    val model = new Join(sc)
    val params = model.parseParams(args_test)
    model.run(params)

    val args_test2 = Seq("--leftTableName","join_left","--rightTableName","join_right","--outputTableName","join_output1",
      "--joinMethod","INNER","--joinConditions","ds=ds","--leftOutFieldsName","x1,x_1\tx2,x_2\tds,ds",
      "--rightOutFieldsName","x1,x1\tx2,x2").toArray

    val params2 = model.parseParams(args_test2)
    model.run(params2)


    val args_test3 = Seq("--leftTableName","join_left","--rightTableName","join_right","--outputTableName","join_output2",
      "--joinMethod","LEFT","--joinConditions","ds=ds","--leftOutFieldsName","x1,x_1\tx2,x_2\tds,ds",
      "--rightOutFieldsName","x1,x1\tx2,x2").toArray

    val params3 = model.parseParams(args_test3)
    model.run(params3)

    val args_test4 = Seq("--leftTableName","join_left","--rightTableName","join_right","--outputTableName","join_output3",
      "--joinMethod","RIGHT","--joinConditions","ds=ds","--leftOutFieldsName","x1,x_1\tx2,x_2\tds,ds",
      "--rightOutFieldsName","x1,x1\tx2,x2").toArray

    val params4 = model.parseParams(args_test4)
    model.run(params4)


    val args_test5 = Seq("--leftTableName","join_left","--rightTableName","join_right","--outputTableName","join_output4",
      "--joinMethod","FULL","--joinConditions","ds=ds","--leftOutFieldsName","x1,x_1\tx2,x_2\tds,ds",
      "--rightOutFieldsName","x1,x1\tx2,x2").toArray


    val params5 = model.parseParams(args_test5)
    model.run(params5)
  }

}
