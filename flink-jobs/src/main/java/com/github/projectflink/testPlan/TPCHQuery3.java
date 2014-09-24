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


package com.github.projectflink.testPlan;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * This program implements a modified version of the TPC-H query 3. The
 * example demonstrates how to assign names to fields by extending the Tuple class.
 * The original query can be found at
 * <a href="http://www.tpc.org/tpch/spec/tpch2.16.0.pdf">http://www.tpc.org/tpch/spec/tpch2.16.0.pdf</a> (page 29).
 *
 * <p>
 * This program implements the following SQL equivalent:
 *
 * <p>
 * <code><pre>
 * SELECT 
 *      l_orderkey, 
 *      SUM(l_extendedprice*(1-l_discount)) AS revenue,
 *      o_orderdate, 
 *      o_shippriority
 * FROM customer, 
 *      orders, 
 *      lineitem 
 * WHERE
 *      c_mktsegment = '[SEGMENT]' 
 *      AND c_custkey = o_custkey
 *      AND l_orderkey = o_orderkey
 *      AND o_orderdate < date '[DATE]'
 *      AND l_shipdate > date '[DATE]'
 * GROUP BY
 *      l_orderkey, 
 *      o_orderdate, 
 *      o_shippriority;
 * </pre></code>
 *
 * <p>
 * Compared to the original TPC-H query this version does not sort the result by revenue
 * and orderdate.
 *
 * <p>
 * Input files are plain text CSV files using the pipe character ('|') as field separator 
 * as generated by the TPC-H data generator which is available at <a href="http://www.tpc.org/tpch/">http://www.tpc.org/tpch/</a>.
 *
 *  <p>
 * Usage: <code>TPCHQuery3 &lt;lineitem-csv path&gt; &lt;customer-csv path&gt; &lt;orders-csv path&gt; &lt;result path&gt;</code><br>
 *
 * <p>
 * This example shows how to use:
 * <ul>
 * <li> custom data type derived from tuple data types
 * <li> inline-defined functions
 * <li> build-in aggregation functions
 * </ul>
 */
@SuppressWarnings("serial")
public class TPCHQuery3 {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<Lineitem> li = getLineitemDataSet(env);
		DataSet<Order> or = getOrdersDataSet(env);
		DataSet<Customer> cust = getCustomerDataSet(env);

		// Filter market segment "AUTOMOBILE"
		cust = cust.filter(
			new FilterFunction<Customer>() {
				@Override
				public boolean filter(Customer value) {
					return value.getMktsegment().equals("AUTOMOBILE");
				}
			});

		// Filter all Orders with o_orderdate < 12.03.1995
		or = or.filter(
			new FilterFunction<Order>() {
				private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				private Date date;

				{
					Calendar cal = Calendar.getInstance();
					cal.set(1995, 3, 12);
					date = cal.getTime();
				}

				@Override
				public boolean filter(Order value) throws ParseException {
					Date orderDate = format.parse(value.getOrderdate());
					return orderDate.before(date);
				}
			});

		// Filter all Lineitems with l_shipdate > 12.03.1995
		li = li.filter(
			new FilterFunction<Lineitem>() {
				private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				private Date date;

				{
					Calendar cal = Calendar.getInstance();
					cal.set(1995, 3, 12);
					date = cal.getTime();
				}

				@Override
				public boolean filter(Lineitem value) throws ParseException {
					Date shipDate = format.parse(value.getShipdate());
					return shipDate.after(date);
				}
			});

		// Join customers with orders and package them into a ShippingPriorityItem
		DataSet<ShippingPriorityItem> customerWithOrders =
			cust.join(or)
				.where(0)
				.equalTo(1)
				.with(
					new JoinFunction<Customer, Order, ShippingPriorityItem>() {
						@Override
						public ShippingPriorityItem join(Customer first, Order second) {
							return new ShippingPriorityItem(0, 0.0, second.getOrderdate(),
								second.getShippriority(), second.getOrderkey());
						}
					});

		// Join the last join result with Lineitems
		DataSet<ShippingPriorityItem> joined =
			customerWithOrders.join(li)
				.where(4)
				.equalTo(0)
				.with(
					new JoinFunction<ShippingPriorityItem, Lineitem, ShippingPriorityItem>() {
						@Override
						public ShippingPriorityItem join(ShippingPriorityItem first, Lineitem second) {
							first.setL_Orderkey(second.getOrderkey());
							first.setRevenue(second.getExtendedprice() * (1 - second.getDiscount()));
							return first;
						}
					});

		// Group by l_orderkey, o_orderdate and o_shippriority and compute revenue sum
		joined = joined
			.groupBy(0, 2, 3)
			.aggregate(Aggregations.SUM, 1);

		// emit result
		joined.writeAsCsv(outputPath, "\n", "|");

		// execute program
		env.execute("TPCH Query 3 Example");

	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	public static class Lineitem extends Tuple4<Integer, Double, Double, String> {

		public Integer getOrderkey() { return this.f0; }
		public Double getDiscount() { return this.f2; }
		public Double getExtendedprice() { return this.f1; }
		public String getShipdate() { return this.f3; }
	}

	public static class Customer extends Tuple2<Integer, String> {

		public Integer getCustKey() { return this.f0; }
		public String getMktsegment() { return this.f1; }
	}

	public static class Order extends Tuple4<Integer, Integer, String, Integer> {

		public Integer getOrderkey() { return this.f0; }
		public Integer getCustKey() { return this.f1; }
		public String getOrderdate() { return this.f2; }
		public Integer getShippriority() { return this.f3; }
	}

	public static class ShippingPriorityItem extends Tuple5<Integer, Double, String, Integer, Integer> {

		public ShippingPriorityItem() { }

		public ShippingPriorityItem(Integer l_orderkey, Double revenue,
									String o_orderdate, Integer o_shippriority, Integer o_orderkey) {
			this.f0 = l_orderkey;
			this.f1 = revenue;
			this.f2 = o_orderdate;
			this.f3 = o_shippriority;
			this.f4 = o_orderkey;
		}

		public Integer getL_Orderkey() { return this.f0; }
		public void setL_Orderkey(Integer l_orderkey) { this.f0 = l_orderkey; }
		public Double getRevenue() { return this.f1; }
		public void setRevenue(Double revenue) { this.f1 = revenue; }

		public String getOrderdate() { return this.f2; }
		public Integer getShippriority() { return this.f3; }
		public Integer getO_Orderkey() { return this.f4; }
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static String lineitemPath;
	private static String customerPath;
	private static String ordersPath;
	private static String outputPath;

	private static boolean parseParameters(String[] programArguments) {

		if(programArguments.length > 0) {
			if(programArguments.length == 4) {
				lineitemPath = programArguments[0];
				customerPath = programArguments[1];
				ordersPath = programArguments[2];
				outputPath = programArguments[3];
			} else {
				System.err.println("Usage: TPCHQuery3 <lineitem-csv path> <customer-csv path> <orders-csv path> <result path>");
				return false;
			}
		} else {
			System.err.println("This program expects data from the TPC-H benchmark as input data.\n" +
				"  Due to legal restrictions, we can not ship generated data.\n" +
				"  You can find the TPC-H data generator at http://www.tpc.org/tpch/.\n" +
				"  Usage: TPCHQuery3 <lineitem-csv path> <customer-csv path> <orders-csv path> <result path>");
			return false;
		}
		return true;
	}

	private static DataSet<Lineitem> getLineitemDataSet(ExecutionEnvironment env) {
		return env.readCsvFile(lineitemPath)
			.fieldDelimiter('|')
			.includeFields("1000011000100000")
			.tupleType(Lineitem.class);
	}

	private static DataSet<Customer> getCustomerDataSet(ExecutionEnvironment env) {
		return env.readCsvFile(customerPath)
			.fieldDelimiter('|')
			.includeFields("10000010")
			.tupleType(Customer.class);
	}

	private static DataSet<Order> getOrdersDataSet(ExecutionEnvironment env) {
		return env.readCsvFile(ordersPath)
			.fieldDelimiter('|')
			.includeFields("110010010")
			.tupleType(Order.class);
	}

}
