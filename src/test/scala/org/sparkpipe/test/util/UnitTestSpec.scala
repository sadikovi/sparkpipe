package org.sparkpipe.test.util

import org.scalatest._
import org.scalatest.{BeforeAndAfterAll => BaseBeforeAndAfterAll}

/** abstract general testing class */
abstract class UnitTestSpec extends FunSuite with Matchers with OptionValues with Inside with Inspectors with Base
