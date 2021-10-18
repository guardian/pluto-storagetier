package com.gu.multimedia.storagetier.framework

import org.specs2.mutable.Specification

class RoutingKeyMatcherSpec extends Specification {
  "RoutingKeyMatcher.checkMatch" should {
    "return true for two matching strings" in {
      RoutingKeyMatcher.checkMatch("some.routing.key",Seq("some","routing","key")) must beTrue
    }

    "return true for two strings that match on a * matcher" in {
      RoutingKeyMatcher.checkMatch("some.*.key",Seq("some","routing","key")) must beTrue
    }

    "return true for two strings that match on a # matcher" in {
      RoutingKeyMatcher.checkMatch("some.#",Seq("some","routing","key")) must beTrue
    }

    "return true for a single # keyspec" in {
      RoutingKeyMatcher.checkMatch("#",Seq("some","routing","key")) must beTrue
    }

    "return true for two strings that match on mixed # matcher" in {
      RoutingKeyMatcher.checkMatch("some.routing.*.that.#",Seq("some","routing","key","that","is","long")) must beTrue
    }

    "return false for two non-matching strings" in {
      RoutingKeyMatcher.checkMatch("some.different.thing",Seq("some","routing","key")) must beFalse
    }

    "return false for two strings that only differ in the first section" in {
      RoutingKeyMatcher.checkMatch("another.routing.key",Seq("some","routing","key")) must beFalse
    }

    "return false for two non-matching strings with a * wildcard" in {
      RoutingKeyMatcher.checkMatch("some.*.thing",Seq("some","routing","key")) must beFalse
    }

    "return false for two non-matching strings with a # wildcard" in {
      RoutingKeyMatcher.checkMatch("some.different.#",Seq("some","routing","key")) must beFalse
    }
  }
}
