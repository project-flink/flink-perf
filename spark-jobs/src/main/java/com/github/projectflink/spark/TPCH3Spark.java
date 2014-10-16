package com.github.projectflink.spark;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.github.projectflink.spark.util.TPCH3ScalaReg;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;


/**
 * local[4] file:///home/robert/flink-workdir/flink-perf/automation/workdir/testjob-data/lineitem.tbl file:///home/robert/flink-workdir/flink-perf/automation/workdir/testjob-data/order.tbl  file:///home/robert/flink-workdir/flink-perf/automation/workdir/testjob-data/customer.tbl  file:///home/robert/flink-workdir/flink-perf/localsparkout/tpch3/
 * 
 * 
 *
 */

public class TPCH3Spark {
	
	// *************************************************************************
	//     PROGRAM
	// *************************************************************************
	public static String SPLIT = "\\|";
	public static void main(String[] args) throws Exception {
		String master = args[0];
		String lineitem = args[1];
		String order = args[2];
		String customer = args[3];
		String output = args[4];
		System.err.println("Starting spark with master="+master);
		

		SparkConf conf = new SparkConf().setAppName("TPCH 3").setMaster(master).set("spark.hadoop.validateOutputSpecs", "false");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", TPCH3ScalaReg.class.getName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// get input data
		JavaRDD<String> liStr = sc.textFile(lineitem);
		JavaRDD<String> orStr =  sc.textFile(order);
		JavaRDD<String> custStr =  sc.textFile(customer);
		
		JavaPairRDD<Integer, Lineitem> li = liStr.mapToPair(new PairFunction<String, Integer, Lineitem>() {
			@Override
			public Tuple2<Integer, Lineitem> call(String t) throws Exception {
				String[] el = t.split(SPLIT);
				// 1000011000100000
				// 0123456789012345
				// 0    12   3
				System.err.println("Line item keys = "+el[0]);
				Lineitem li = new Lineitem(Integer.getInteger(el[0]), Double.valueOf(el[6]), 
						Double.valueOf(el[5]), el[10]);
				return new Tuple2<Integer, TPCH3Spark.Lineitem>(li.getOrderkey(), li);
			}
		});

		JavaPairRDD<Integer, Order> or = orStr.mapToPair(new PairFunction<String, Integer, Order>() {
			@Override
			public Tuple2<Integer, Order> call(String t) throws Exception {
				String[] el = t.split(SPLIT);
				// 100010010
				// 012345678
				Order o = new Order(Integer.valueOf(el[0]), el[4], Integer.valueOf(el[7]));
				return new Tuple2<Integer, Order>(o.getOrderkey(), o);
			}
		});
		
		JavaPairRDD<Integer, Customer> cust = custStr.mapToPair(new PairFunction<String, Integer, Customer>() {

			@Override
			public Tuple2<Integer, Customer> call(String t) throws Exception {
				String[] el = t.split(SPLIT);
				// 10000010
				// 01234567
				Customer c = new Customer(Integer.valueOf(el[0]), el[6]);
				return new Tuple2<Integer, Customer> (c.getCustKey(), c);
			}});
		
		// Filter market segment "AUTOMOBILE"
		cust = cust.filter( new Function<Tuple2<Integer,Customer>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Integer, Customer> v1) throws Exception {
				return v1._2().getMktsegment().equals("AUTOMOBILE");
			}}
		);

		// Filter all Orders with o_orderdate < 12.03.1995
		or = or.filter( new Function<Tuple2<Integer,Order>, Boolean>() {
			private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			private Date date;

			{
				Calendar cal = Calendar.getInstance();
				cal.set(1995, 3, 12);
				date = cal.getTime();
			}
			@Override
			public Boolean call(Tuple2<Integer, Order> v1) throws Exception {
				Date orderDate = format.parse(v1._2().getOrderdate());
				return orderDate.before(date);
			}} 
		);
		

		// Filter all Lineitems with l_shipdate > 12.03.1995
		li = li.filter(new Function<Tuple2<Integer,Lineitem>, Boolean>() {
				private DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				private Date date;

				{
					Calendar cal = Calendar.getInstance();
					cal.set(1995, 3, 12);
					date = cal.getTime();
				}

				@Override
				public Boolean call(Tuple2<Integer,Lineitem> value) throws ParseException {
					Date shipDate = format.parse(value._2().getShipdate());
					return shipDate.after(date);
				}
			}
		);
		
		System.err.println("++++++ Li count = "+li.count()+" cust count = "+cust.count()+" orders count = "+or.count());
		
		JavaPairRDD<Integer, ShippingPriorityItem> customerWithOrders = cust.join(or)
				// set orderkey to key (for upcoming join)
				.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Customer,Order>>, Integer, ShippingPriorityItem>() {
					@Override
					public Tuple2<Integer, ShippingPriorityItem> call(
							Tuple2<Integer, Tuple2<Customer, Order>> t)
							throws Exception {
						final Order second = t._2()._2();
						ShippingPriorityItem spi = new ShippingPriorityItem(0, 0.0, second.getOrderdate(),
								second.getShippriority(), second.getOrderkey());
						System.err.println("C with O keys = "+second.getOrderkey());
						return new Tuple2<Integer, ShippingPriorityItem>(second.getOrderkey(), spi);
					}
					/**
					 * new JoinFunction<Customer, Order, ShippingPriorityItem>() {
						@Override
						public ShippingPriorityItem join(Customer first, Order second) {
							return new ShippingPriorityItem(0, 0.0, second.getOrderdate(),
								second.getShippriority(), second.getOrderkey());
						}
					});
					 */
		});
		
		System.err.println("++++++ customerWithOrders count "+customerWithOrders.count());
		
		JavaPairRDD<Integer, Tuple2<ShippingPriorityItem, Lineitem>> joined = customerWithOrders.join(li);
		
		System.err.println("++++++ joined count "+joined.count());
		
		// .groupBy(0, 2, 3)
		JavaPairRDD<Tuple3<Integer, String, Integer>, ShippingPriorityItem> joined1 = joined.mapToPair(
				new PairFunction<Tuple2<Integer,Tuple2<ShippingPriorityItem,Lineitem>>, Tuple3<Integer, String, Integer>, ShippingPriorityItem>() {
					@Override
					public Tuple2<Tuple3<Integer, String, Integer>, ShippingPriorityItem> call(
							Tuple2<Integer, Tuple2<ShippingPriorityItem, Lineitem>> t)
							throws Exception {
						final ShippingPriorityItem spi = t._2()._1();
						final Lineitem second = t._2()._2();
						ShippingPriorityItem spiImmu = new ShippingPriorityItem(second.getOrderkey(), 
								second.getExtendedprice() * (1 - second.getDiscount()), spi._3(), spi._4(), spi._5());
						return new Tuple2<Tuple3<Integer, String, Integer>, ShippingPriorityItem>(new Tuple3<Integer, String, Integer>(spiImmu._1(),spiImmu._3(),spiImmu._4()), 
								spiImmu);
					}
		});
		
		System.err.println("++++++ joined1 count "+joined1.count());
		
		JavaPairRDD<Tuple3<Integer, String, Integer>, ShippingPriorityItem> finalDs = joined1.reduceByKey(new Function2<TPCH3Spark.ShippingPriorityItem, TPCH3Spark.ShippingPriorityItem, TPCH3Spark.ShippingPriorityItem>() {
			
			@Override
			public ShippingPriorityItem call(
					ShippingPriorityItem v1, ShippingPriorityItem v2)
					throws Exception {
				return new ShippingPriorityItem(v1._1(), v1._2()+v2._2(), v1._3(), v1._4(), v1._5());
			}
		});
		
		System.err.println("++++++ finalDs count "+finalDs.count());
		
		finalDs.saveAsTextFile(output);

		// Join the last join result with Lineitems
	/*	DataSet<ShippingPriorityItem> joined =
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
					}); */

		// Group by l_orderkey, o_orderdate and o_shippriority and compute revenue sum
	/*	joined = joined
			.groupBy(0, 2, 3)
			.aggregate(Aggregations.SUM, 1);

		// emit result
		joined.writeAsCsv(outputPath, "\n", "|");
 */

	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	public static class Lineitem extends Tuple4<Integer, Double, Double, String> {
		
		public Lineitem(Integer arg0, Double arg1, Double arg2, String arg3) {
			super(arg0, arg1, arg2, arg3);
		}
		public Integer getOrderkey() { return this._1(); }
		public Double getDiscount() { return this._3(); }
		public Double getExtendedprice() { return this._2(); }
		public String getShipdate() { return this._4(); }
	}

	public static class Customer extends Tuple2<Integer, String> {

		public Customer(Integer arg0, String arg1) {
			super(arg0, arg1);
		}
		public Integer getCustKey() { return this._1(); }
		public String getMktsegment() { return this._2(); }
	}

	public static class Order extends Tuple3<Integer, String, Integer> {

		public Order(Integer arg0, String arg1, Integer arg2) {
			super(arg0, arg1, arg2);
		}
		public Integer getOrderkey() { return this._1(); }
		public String getOrderdate() { return this._2(); }
		public Integer getShippriority() { return this._3(); }
	}

	
															// 0      1        2    	3		4
	public static class ShippingPriorityItem extends Tuple5<Integer, Double, String, Integer, Integer> {


		public ShippingPriorityItem(Integer arg0, Double arg1, String arg2,
				Integer arg3, Integer arg4) {
			super(arg0, arg1, arg2, arg3, arg4);
		}
		public Integer getL_Orderkey() { return this._1(); }
	//	public void setL_Orderkey(Integer l_orderkey) { this.f0 = l_orderkey; }
		public Double getRevenue() { return this._2(); }
	//	public void setRevenue(Double revenue) { this.f1 = revenue; }

		public String getOrderdate() { return this._3(); }
		public Integer getShippriority() { return this._4(); }
		public Integer getO_Orderkey() { return this._5(); }
		
		@Override
		public String toString() {
			return this._1()+"|"+this._2()+"|"+this._3()+"|"+this._4()+"|"+this._5()+"\n";
		}
	}

}
