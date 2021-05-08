package org.renci.cam

import zio.test.Assertion._
import zio.test._
import Utilities.ListOps

object TestUtilities extends DefaultRunnableSpec {

  override def spec = suite("TestUtilities")(
    test("Test List ops") {
      val names = List("Bart", "Lisa", "Homer")
      val interspersed = names.intersperse(",")
      assert(interspersed)(equalTo(List("Bart", ",", "Lisa", ",", "Homer")))
    }
  )

}
