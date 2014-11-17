package com.github.projectflink.spark;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

/**
* Created by robert on 10/9/14.
*/

public class MyRegistrator implements KryoRegistrator {

	public MyRegistrator() {

	}
	public void registerClasses(Kryo kryo) {
		kryo.register(KMeansArbitraryDimension.Point.class);
	}

}
