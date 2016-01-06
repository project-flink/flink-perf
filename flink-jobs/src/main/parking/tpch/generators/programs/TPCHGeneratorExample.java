/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.projectflink.generators.tpch.generators.programs;

import com.github.projectflink.generators.tpch.generators.core.DistributedTPCH;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;


/**
 * TPC H Schema Diagramm: https://www.student.cs.uwaterloo.ca/~cs348/F14/TPC-H_Schema.png
 */
public class TPCHGeneratorExample {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DistributedTPCH gen = new DistributedTPCH(env);
		gen.setScale(1.0);

		DataSet<Order> orders = gen.generateOrders();

		DataSet<LineItem> lineitem = gen.generateLineItems();

		DataSet<Tuple2<Order, LineItem>> oxl = orders.join(lineitem)
				.where(selectKey("orderKey", Order.class)).equalTo(selectKey("orderKey", LineItem.class));
		
		oxl.print();
		// execute program
		env.execute("Flink Java API Skeleton");
	}
	
	//
	// --------- Hacky Key selection, allowing easy POJO usage also for GenericTypes. --
	//
	
	@SuppressWarnings("rawtypes")
	public static KeySelector selectKey(final String field, Class clazz) {
		return new CheapKeySelector(field, clazz);
	}
	
	@SuppressWarnings("rawtypes")
	private static class CheapKeySelector implements KeySelector, ResultTypeQueryable {
		private transient TypeInformation ti;
		private Class clazz;
		private transient Field cField;
		private String field;
		public CheapKeySelector(String field, Class clazz) {
			this.clazz = clazz;
			this.field = field;
			try {
				Field cField = clazz.getDeclaredField(field);
				ti = TypeExtractor.createTypeInfo(cField.getType());
				cField.setAccessible(true);
			} catch (Throwable e) {
				throw new RuntimeException("Unable to find field "+field+" in class "+clazz);
			}
		}
		
		@Override
		public TypeInformation getProducedType() {
			return ti;
		}
		
		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException, NoSuchFieldException, SecurityException {
			in.defaultReadObject();
			cField = clazz.getDeclaredField(field);
			cField.setAccessible(true);
		}
		
		@Override
		public Object getKey(Object value) throws Exception {
			return cField.get(value);
		}
	}

}
