package com.github.projectflink.spark.util

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import com.github.projectflink.spark.TPCH3Spark.Lineitem
import com.github.projectflink.spark.TPCH3Spark.Customer
import com.github.projectflink.spark.TPCH3Spark.Order
import com.github.projectflink.spark.TPCH3Spark.ShippingPriorityItem

class TPCH3ScalaReg() extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Lineitem]);
    kryo.register(classOf[Customer]);
    kryo.register(classOf[Order]);
    kryo.register(classOf[ShippingPriorityItem]);
  }
}