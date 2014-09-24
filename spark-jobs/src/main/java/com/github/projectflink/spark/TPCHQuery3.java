package com.github.projectflink.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TPCHQuery3 {
	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		SparkConf conf = new SparkConf().setAppName("TPCH 3").setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);

		// get input data
		JavaRDD<Lineitem> lineitem = sc
			.textFile(lineitemPath)
			.map(new Function<String, Lineitem>() {
				@Override
				public Lineitem call(String s) throws Exception {
					return new Lineitem(s);
				}
			});
		JavaRDD<Customer> customer = sc
			.textFile(customerPath)
			.map(new Function<String, Customer>() {
				@Override
				public Customer call(String s) throws Exception {
					return new Customer(s);
				}
			});
		JavaRDD<Order> order = sc
			.textFile(ordersPath)
			.map(new Function<String, Order>() {
				@Override
				public Order call(String s) throws Exception {
					return new Order(s);
				}
			});

		// Filter market segment "AUTOMOBILE"
		customer = customer.filter(new Function<Customer, Boolean>() {
			@Override
			public Boolean call(Customer c) throws Exception {
				return c.mktSegment.equals("AUTOMOBILE");
			}
		});

		// Filter all Orders with o_orderdate < 12.03.1995
		order = order.filter(new Function<Order, Boolean>() {
			private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			private Date date;

			{
				Calendar cal = Calendar.getInstance();
				cal.set(1995, 3, 12);
				date = cal.getTime();
			}
			@Override
			public Boolean call(Order o) throws Exception {
				Date orderDate = format.parse(o.orderDate);
				return orderDate.before(date);
			}
		});

		// Filter all Lineitems with l_shipdate > 12.03.1995
		lineitem = lineitem.filter(new Function<Lineitem, Boolean>() {
			private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			private Date date;

			{
				Calendar cal = Calendar.getInstance();
				cal.set(1995, 3, 12);
				date = cal.getTime();
			}
			@Override
			public Boolean call(Lineitem l) throws Exception {
				Date shipDate = format.parse(l.shipDate);
				return shipDate.after(date);			}
		});


		// Join customers with orders and package them into a ShippingPriorityItem
		JavaRDD<Order> customerWithOrders = customer
			.keyBy(new Function<Customer, Integer>() {
				@Override
				public Integer call(Customer customer) throws Exception {
					return customer.custKey;
				}
			})
			.join(order.keyBy(new Function<Order, Integer>() {
				@Override
				public Integer call(Order order) throws Exception {
					return order.custKey;
				}
			}))
			.map(new Function<Tuple2<Integer, Tuple2<Customer, Order>>, Order>() {
				@Override
				public Order call(Tuple2<Integer, Tuple2<Customer, Order>> t) throws Exception {
					return t._2()._2();
				}
			});

		// Join the last join result with Lineitems
		JavaRDD<ShipPriorityItem> joined = customerWithOrders
			.keyBy(new Function<Order, Integer>() {
				@Override
				public Integer call(Order o) throws Exception {
					return o.orderKey;
				}
			})
			.join(lineitem.keyBy(new Function<Lineitem, Integer>() {
				@Override
				public Integer call(Lineitem l) throws Exception {
					return l.orderKey;
				}
			}))
			.map(new Function<Tuple2<Integer, Tuple2<Order, Lineitem>>, ShipPriorityItem>() {
				@Override
				public ShipPriorityItem call(Tuple2<Integer, Tuple2<Order, Lineitem>> t) throws Exception {
					return new ShipPriorityItem(t._2()._1(), t._2()._2());
				}
			});


		// Join customers with orders and package them into a ShippingPriorityItem
		JavaPairRDD<Tuple3<Integer, String, Integer>, Double> result = joined
			.mapToPair(new PairFunction<ShipPriorityItem, Tuple3<Integer, String, Integer>, Double>() {
				@Override
				public Tuple2<Tuple3<Integer, String, Integer>, Double> call(ShipPriorityItem s) throws Exception {
					return new Tuple2<Tuple3<Integer, String, Integer>, Double>(new Tuple3<Integer, String, Integer>(s.orderKey, s.orderDate, s.shipPriority), s.revenue);
				}
			})
			.reduceByKey(new Function2<Double, Double, Double>() {
				@Override
				public Double call(Double d1, Double d2) throws Exception {
					return d1 + d2;
				}
			});

		result.saveAsTextFile(outputPath);

		
	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	public static class Lineitem implements Serializable {

		public Integer orderKey;
		public Double discount;
		public Double extendedPrice;
		public String shipDate;

		public Lineitem(String s) {
			String [] line = s.split("\\|");
 			this.orderKey = Integer.parseInt(line[0]);
			this.discount = Double.parseDouble(line[6]);
			this.extendedPrice = Double.parseDouble(line[5]);
			this.shipDate = line[10];
		}
	}

	public static class Customer implements Serializable {

		public Integer custKey;
		public String mktSegment;

		public Customer(String s) {
			String [] line = s.split("\\|");
			this.custKey = Integer.parseInt(line[0]);
			this.mktSegment = line[6];
		}

	}


	public static class Order implements Serializable {

		public Integer orderKey;
		public Integer custKey;
		public String orderDate;
		public Integer shipPriority;

		public Order(String s) {
			String [] line = s.split("\\|");
			this.orderKey = Integer.parseInt(line[0]);
			this.custKey = Integer.parseInt(line[1]);
			this.orderDate = line[4];
			this.shipPriority = Integer.parseInt(line[7]);

		}

	}

	public static class ShipPriorityItem implements Serializable {

		public Integer orderKey;
		public String orderDate;
		public Double revenue;
		public Integer shipPriority;


		public ShipPriorityItem(Order o, Lineitem l) {
			this.orderKey = o.orderKey;
			this.orderDate = o.orderDate;
			this.shipPriority = o.shipPriority;
			this.revenue = 	l.extendedPrice * (1 - l.discount);
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static String master;
	private static String lineitemPath;
	private static String customerPath;
	private static String ordersPath;
	private static String outputPath;

	private static boolean parseParameters(String[] programArguments) {

		if(programArguments.length == 5) {
			master = programArguments[0];
			lineitemPath = programArguments[1];
			customerPath = programArguments[2];
			ordersPath = programArguments[3];
			outputPath = programArguments[4];
		} else {
			System.err.println("Usage: TPCHQuery3 <master> <lineitem-csv path> <customer-csv path> <orders-csv path> <result path>");
			return false;
		}
		return true;
	}
}
