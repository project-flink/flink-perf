/*
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

package com.github.projectflink.testPlan;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.DiscardingOuputFormat;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.fs.shell.Count;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

@SuppressWarnings("serial")
public class KryoTest {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {


		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<String> text = env.readTextFile(args[0]);
		boolean kryoMode = false;

		if(args[1].equals("kryo")) {
			kryoMode = true;
			DataSet<KryoType> ds = text.map(new MapFunction<String, KryoType>() {
				@Override
				public KryoType map(String s) throws Exception {
					KryoType kt = new KryoType();
					kt.elements = new ArrayList<Object>();
					String[] splits = s.split(" ");
					for (String split : splits) {
						if(split == null || split.length() == 0) continue;
						if (StringUtils.isNumeric(split)) {
							kt.elements.add(Integer.valueOf(split));
						} else {
							kt.elements.add(split);
						}
					}
					return kt;
				}
			});
			DataSet<Tuple2<Object, Integer>> ds1 = ds.rebalance().map(new MapFunction<KryoType, Tuple2<Object, Integer>>() {
				@Override
				public Tuple2<Object, Integer> map(KryoType valueType) throws Exception {
					return new Tuple2<Object, Integer>(valueType.elements.iterator().next(), valueType.elements.size());
				}
			});
			ds1.print();
			ds.output(new DiscardingOuputFormat<KryoType>());
		} else {
			DataSet<ValueType> ds = text.map(new MapFunction<String, ValueType>() {
				@Override
				public ValueType map(String s) throws Exception {
					ValueType vt = new ValueType();
					vt.elements = new ArrayList<Object>();
					String[] splits = s.split(" ");
					for (String split : splits) {
						if(split == null || split.length() == 0) continue;
						if (StringUtils.isNumeric(split)) {
							vt.elements.add(Integer.valueOf(split));
						} else {
							vt.elements.add(split);
						}
					}
					return vt;
				}
			});
			DataSet<Tuple2<Object, Integer>> ds1 = ds.rebalance().map(new MapFunction<ValueType, Tuple2<Object, Integer>>() {
				@Override
				public Tuple2<Object, Integer> map(ValueType valueType) throws Exception {
					return new Tuple2<Object, Integer>(valueType.elements.iterator().next(), valueType.elements.size());
				}
			});
			ds1.print();
			ds.output(new DiscardingOuputFormat<ValueType>());
		}

		// execute program
		env.execute("KryoTest kryoMode=" + kryoMode);
	}


	public static class Countable {
		public Collection<Object> elements;

		public int getCollectionSize() {
			return  elements.size();
		}
	}

	public static class KryoType extends Countable {
		@Override
		public String toString() {
			return "KryoType{" +
					"elements=" + elements +
					'}';
		}
	}

	public static class ValueType extends Countable implements Value {

		public ValueType() {
			elements = new ArrayList<Object>();
		}
		@Override
		public void write(DataOutputView dataOutputView) throws IOException {
			dataOutputView.writeInt(elements.size());
			for(Object e: elements) {
				if(e instanceof Integer) {
					dataOutputView.writeInt(0);
					dataOutputView.writeInt((Integer) e);
				} else {
					dataOutputView.writeInt(1);
					dataOutputView.writeUTF((String) e);
				}
			}
		}

		@Override
		public void read(DataInputView dataInputView) throws IOException {
			elements.clear();
			int size = dataInputView.readInt();
			for(int i = 0; i < size; i++) {
				int type = dataInputView.readInt();
				if (type == 0) {
					elements.add(dataInputView.readInt());
				} else {
					elements.add(dataInputView.readUTF());
				}
			}
		}


		@Override
		public String toString() {
			return "ValueType{" +
					"elements=" + elements +
					'}';
		}
	}
}
