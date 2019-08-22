package com.lackey.spark.examples.sqlfuns

object TypeFun extends App {

  object Shared {
    trait Foo[T <: Foo[T]] {
      def +(other : T): T
    }

    case class IntFoo(value: Int) extends Foo[IntFoo] {
      def +(other: IntFoo): IntFoo = IntFoo(this.value + other.value)
    }

    case class StringFoo(value: String) extends Foo[StringFoo] {
      def +(other: StringFoo): StringFoo = StringFoo(this.value + other.value)
    }

  }

  import Shared._

  case class Bar[T <: Foo[T]](foo: T) {
    def +(other: Bar[T]): Bar[T] = Bar[T](foo + other.foo)
  }

  val bar1 = Bar(IntFoo(2))
  val bar2 = Bar(IntFoo(3))
  val expected = Bar(IntFoo(5))
  require(bar1 + bar2 == expected)
  println("All OK")

  val otherBar = Bar(StringFoo("asdf"))
  //val invalidSum = bar1 + otherBar // this will not compile since  String != Int

}
