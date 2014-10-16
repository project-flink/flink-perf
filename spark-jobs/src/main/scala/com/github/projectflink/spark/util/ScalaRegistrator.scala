package com.github.projectflink.spark.util

import com.esotericsoftware.kryo.Kryo
import com.github.projectflink.spark.KMeansArbitraryDimension
import org.apache.spark.serializer.KryoRegistrator

/**
 * Created by robert on 10/9/14.
 */

class ScalaRegistrator() extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[KMeansArbitraryDimension.Point]);
  }
}
