package eu.stratosphere.test.javaTestPlan;


import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.accumulators.IntCounter;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.CrossFunction;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.io.AvroInputFormat;
//CHECKSTYLE.OFF: AvoidStarImport
import eu.stratosphere.api.java.tuple.*;
//CHECKSTYLE.ON: AvoidStarImport
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.test.testPlan.Order;
import eu.stratosphere.util.Collector;

import java.io.File;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.Scanner;

public class LargeTestPlan {
	public static String customer;
	public static String lineitem;
	public static String nation;
	public static String orders;
	public static String region;
	public static String orderAvroFile;
	public static String outputTableDirectory;
	public static String sequenceFileInput;

	public static int maxBulkIterations;

	// paths (without file:// or hdfs://)
	public static String outputAccumulatorsPath;
	public static String outputKeylessReducerPath;
	public static int num;


	public static void main(String[] args) throws Exception {

		if (args.length >= 11) {
			// Examples for testing
			// file:///Users/qml_moon/Documents/TUB/DIMA/code/testjob/tpch_2_16_0/dbgen/data/customer.tbl
			// file:///Users/qml_moon/Documents/TUB/DIMA/code/testjob/tpch_2_16_0/dbgen/data/lineitem.tbl
			// file:///Users/qml_moon/Documents/TUB/DIMA/code/testjob/tpch_2_16_0/dbgen/data/nation.tbl
			// file:///Users/qml_moon/Documents/TUB/DIMA/code/testjob/tpch_2_16_0/dbgen/data/orders.tbl
			// file:///Users/qml_moon/Documents/TUB/DIMA/code/testjob/tpch_2_16_0/dbgen/data/region.tbl
			// file:///Users/qml_moon/Documents/TUB/DIMA/code/testjob/tpch_2_16_0/dbgen/data/orders.avro
			// file:///Users/qml_moon/Documents/TUB/DIMA/code/testjob/tpch_2_16_0/dbgen/data/directory/
			// 2000
			// /Users/qml_moon/Documents/TUB/DIMA/code/testjob/tpch_2_16_0/dbgen/data/directory/accumulators.txt
			// /Users/qml_moon/Documents/TUB/DIMA/code/testjob/tpch_2_16_0/dbgen/data/directory_new/intermediate-keylessreducer.txt
			// 4
			customer = args[0];
			lineitem = args[1];
			nation = args[2];
			orders = args[3];
			region = args[4];
			orderAvroFile = args[5];
			outputTableDirectory = args[6];
			maxBulkIterations = Integer.valueOf(args[7]);
			// paths (without file:// or hdfs://)
			outputAccumulatorsPath = args[8];
			outputKeylessReducerPath = args[9];
			num = Integer.valueOf(args[10]);
		}
		// error
		else {
			System.err.println(getDescription());
			System.exit(1);
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// Read TPC-H data from .tbl-files
		// (supplier, part and partsupp not implemented yet)
		DataSet<Tuple8<Integer, String, String, Integer, String, Double, String, String>> customerSource = env.readCsvFile(customer)
			.lineDelimiter("\n")
			.fieldDelimiter('|')
			.types(Integer.class, String.class, String.class, Integer.class, String.class, Double.class, String.class, String.class);

		DataSet<Tuple16<Integer, Integer, Integer, Integer, Integer,Float, Float, Float, String, String, String, String, String, String, String, String>> lineitemSource = env.readCsvFile(lineitem)
			.lineDelimiter("\n")
			.fieldDelimiter('|')
			.types(Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, Float.class, Float.class, Float.class, String.class,
				String.class, String.class, String.class, String.class, String.class, String.class, String.class);

		DataSet<Tuple4<Integer, String, Integer, String>> nationSource = env.readCsvFile(nation)
			.lineDelimiter("\n")
			.fieldDelimiter('|')
			.types(Integer.class, String.class, Integer.class, String.class);

		DataSet<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> ordersSource = env.readCsvFile(orders)
			.lineDelimiter("\n")
			.fieldDelimiter('|')
			.types(Integer.class, Integer.class, String.class, Double.class, String.class, String.class, String.class, Integer.class, String.class);

		DataSet<Tuple3<Integer, String, String>> regionSource = env.readCsvFile(region)
			.lineDelimiter("\n")
			.fieldDelimiter('|')
			.types(Integer.class, String.class, String.class);

		// BEGIN: TEST 1 - Usage of Join, Filter, Map, KeylessReducer, CsvOutputFormat, union, CoGroup

		// Join fields of customer and nation
		DataSet<Tuple12<Integer, String, String, Integer, String, Double, String, String,Integer, String, Integer, String>> customerWithNation = customerSource.join(nationSource)
			.where(3).equalTo(0).with(new JoinFields1());

		// Join fields of customerWithNation and region
		DataSet<Tuple15<Integer, String, String, Integer, String, Double, String, String,Integer, String, Integer, String, Integer, String, String>> customerWithNationRegion = customerWithNation.join(regionSource)
			.where(10).equalTo(0).with(new JoinFields2());

		// Split the customers by regions
		DataSet<Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>> customerInAmerica = customerWithNationRegion.filter(new FilterRegion("AMERICA"));
		DataSet<Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>> customerInEuropa = customerWithNationRegion.filter(new FilterRegion("EUROPE"));
		DataSet<Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>> customersInOtherRegions = customerWithNationRegion.filter(new FilterRegionOthers());

		// Count customers of other regions
		DataSet<Tuple1<Integer>> countCustomersOfOtherRegion = customersInOtherRegions.project(0).types(Integer.class)
			.reduceGroup(new ReduceCounter());

		// Save keyless reducer results
		countCustomersOfOtherRegion.writeAsCsv(outputTableDirectory + "/intermediate-keylessreducer.txt", "\n","|");

		// Union again and filter customer fields
		DataSet<Tuple8<Integer, String, String, Integer, String, Double, String, String>> unionOfRegions = customerInAmerica.union(customerInEuropa).union(customersInOtherRegions)
			.map(new FilterCustomerFields());

		// Save test results to disk
		unionOfRegions.writeAsCsv(outputTableDirectory + "/Test1.tbl", "\n", "|");

		// Test: Compare to input source
		DataSet<Tuple8<Integer, String, String, Integer, String, Double, String, String>> testCustomerIdentity1 = customerSource.coGroup(unionOfRegions)
			.where(0).equalTo(0).with(new CoGroupTestIdentity8()).name("TEST 1");


		// END: TEST 1

		// BEGIN: TEST 2 - Usage of Join, Reduce, Map, Cross, CoGroup, Project

		// Collect customers keys from customers that ever placed orders
		DataSet<Tuple1<Integer>> customersWithOrders = lineitemSource.join(ordersSource)
			.where(0).equalTo(0).with(new CollectCustomerKeysWithOrders());
		DataSet<Tuple1<Integer>> removeDuplicates = customersWithOrders.groupBy(0).reduce(new RemoveDuplicates());

		// Cross LineItems and Orders
		DataSet<Tuple25<Integer, Integer, Integer, Integer, Integer,Float, Float, Float, String, String, String, String, String, String, String, String, Integer, Integer, String, Double, String, String, String, Integer, String>> lineitemsWithOrders = lineitemSource.cross(ordersSource).with(new CrossJoinFields());

		// Filter customer key
		DataSet<Tuple1<Integer>> customerKeyWithOrders2 = lineitemsWithOrders.filter(new FilterCustomerKeyFromLineItemsOrders())
			.project(17).types(Integer.class);


		DataSet<Tuple1<Integer>> removeDuplicates2 = customerKeyWithOrders2.groupBy(0).reduce(new RemoveDuplicates());

		// Save test results to disk
		removeDuplicates2.writeAsCsv(outputTableDirectory + "/Test2.tbl", "\n", "|");


		// Test: Compare customer keys
		DataSet<Tuple1<Integer>> testCustomerIdentity2 = removeDuplicates.coGroup(removeDuplicates2)
			.where(0).equalTo(0).with(new CoGroupTestIdentity1());

		// END: TEST 2

		// BEGIN: TEST 3 - Usage of Delta Iterations to determine customers with no orders

		// Add a flag field to each customer (initial value: false)
		DataSet<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> customersWithFlag = customerSource.map(new AddFlag());

		DeltaIteration<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>, Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> iteration = customersWithFlag.iterateDelta(customersWithFlag, 1000, 0);

		// As input for each iteration
		// Exception otherwise
		DataSet<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> iterationInput = iteration.getWorkset().join(iteration.getSolutionSet())
			.where(0).equalTo(0).with(new WorkSolutionSetJoin()).name("JOIN ITERATION");

		//Pick one customer from working set
		DataSet<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> oneCustomer = iterationInput.reduce(new PickOneRecord());

		// Determine all customers from input with no orders (in this case: check if the picked customer has no orders)
		DataSet<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> customerWithNoOrders = oneCustomer.coGroup(ordersSource)
			.where(0).equalTo(1).with(new CustomersWithNoOrders());

		// Set the flag for the customer with no order
		DataSet<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> customerWithSetFlag = customerWithNoOrders.map(new SetFlag());

		// Remove checked customer from previous working set
		DataSet<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> filteredWorkSet = iteration.getWorkset().coGroup(oneCustomer)
			.where(0).equalTo(0).with(new RemoveCheckedCustomer());

		// Set changed customers (delta)
		DataSet<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> iterationResult = iteration.closeWith(customerWithSetFlag, filteredWorkSet);

		// Remove unflagged customer
		DataSet<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> filteredFlaggedSolutionSet = iterationResult.filter(new FilterFlaggedCustomers());

		// Extract only the customer keys
		DataSet<Tuple1<Integer>> customerKeysWithNoOrders = filteredFlaggedSolutionSet.project(0).types(Integer.class);

		// Save the customers without orders in file
		customerKeysWithNoOrders.writeAsCsv(outputTableDirectory + "/Test3.tbl", "\n", "|");

		// Union all customers WITH orders from previous test with all customers WITHOUT orders
		DataSet<Tuple1<Integer>> unionCustomers = customerKeysWithNoOrders.union(testCustomerIdentity2);

		// Test if unionCustomers contains all customers again
		DataSet<Tuple1<Integer>> testCustomerIdentity3 = unionCustomers.coGroup(testCustomerIdentity1.project(0).types(Integer.class))
			.where(0).equalTo(0).with(new CoGroupTestIdentity1()).name("TEST 3");

		// END: TEST 3

		// BEGIN: TEST 4 - Usage of TextInputFormat

		// Get all order keys by joining with all customers that placed orders from previous test
		DataSet<Tuple1<Integer>> allOrderKeys = ordersSource.join(testCustomerIdentity3)
			.where(1).equalTo(0).with(new OrderKeysFromCustomerKeys());

		// Get the string lines of the orders file
		DataSet<String> ordersTextInputSource = env.readTextFile(orders);

		// Extract order keys out of string lines
		DataSet<Tuple1<Integer>> stringExtractKeys = ordersTextInputSource.map(new ExtractKeysFromTextInput());

		// Save the orders in file
		stringExtractKeys.writeAsCsv(outputTableDirectory + "/Test4.tbl", "\n", "|");

		// Test if extracted values are correct
		DataSet<Tuple1<Integer>> testOrderIdentity = allOrderKeys.coGroup(stringExtractKeys)
			.where(0).equalTo(0).with(new CoGroupTestIdentity1())
			.name("TEST 4");


		// END: TEST 4

		// BEGIN: TEST 5 - Usage of AvroInputFormat

		// extract orders from avro file
		DataSet<Order> ordersAvroInputSource = env.createInput(new AvroInputFormat<Order>(new Path(orderAvroFile), Order.class));

		// Extract keys
		DataSet<Tuple1<Integer>> extractKeys = ordersAvroInputSource.map(new FilterFirstFieldIntKey());

		// Save the order keys in file
		extractKeys.writeAsCsv(outputTableDirectory + "/Test5.tbl", "\n", "|");

		DataSet<Tuple1<Integer>> testOrderIdentity2 = extractKeys.coGroup(testOrderIdentity)
			.where(0).equalTo(0).with(new CoGroupTestIdentity1())
			.name("TEST 5");


		// END: TEST 5


		// BEGIN: TEST 6 - date count

		// Count different order dates
		DataSet<Tuple2<String, Integer>> orderDateCountMap = ordersAvroInputSource.map(new AvroOrderDateCountMap());

		// Sum up
		DataSet<Tuple2<String, Integer>> orderDateCountReduce = orderDateCountMap.groupBy(0).aggregate(Aggregations.SUM, 1);

		// Save the orders in file
		orderDateCountReduce.writeAsCsv(outputTableDirectory + "/Test6.tbl", "\n", "|");

		// do the same with the original orders file

		// Count different order dates
		DataSet<Tuple2<String, Integer>> orderDateCountMap2 = ordersSource.map(new OrderDateCountMap());

		// Sum up
		DataSet<Tuple2<String, Integer>> orderDateCountReduce2 = orderDateCountMap2.groupBy(0).aggregate(Aggregations.SUM, 1);

		// Check if date count is correct
		DataSet<Tuple2<String, Integer>> testOrderIdentity3 = orderDateCountReduce
			.coGroup(orderDateCountReduce2)
			.where(0).equalTo(0).with(new CoGroupTestIdentity2())
			.name("TEST 6");

		// END: TEST 6

		// BEGIN: TEST 7

		// Sum up counts
		DataSet<Tuple1<Integer>> sumUp = testOrderIdentity3.reduce(new SumUpDateCounts())
			.project(1).types(Integer.class);

		// Count all orders
		DataSet<Tuple1<Integer>> orderCount = testOrderIdentity2.reduceGroup(new ReduceCounter());

		// Check if the values are equal
		DataSet<Tuple1<Integer>> testCountOrdersIdentity = sumUp.coGroup(orderCount)
			.where(0).equalTo(0).with(new CoGroupTestIdentity1())
			.name("TEST 7");

		// Write count to disk
		testCountOrdersIdentity.writeAsCsv(outputTableDirectory + "/Test7.tbl", "\n", "|");

		// END: TEST 7

		// BEGIN: TEST 9 - Usage of Broadcast Variables

		// Join Customer and Nation using Broadcast Variables
		DataSet<Tuple12<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String>> broadcastJoinNation = customerSource.flatMap(new BroadcastJoinNation())
			.withBroadcastSet(nationSource, "nation");

		// Join Customer, Nation and Region using Broadcast Variables
		DataSet<Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>> broadcastJoinRegion = broadcastJoinNation.flatMap(new BroadcastJoinRegion())
			.withBroadcastSet(regionSource, "region");

		DataSet<Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>> testEquality = customerWithNationRegion.coGroup(broadcastJoinRegion)
			.where(0).equalTo(0).with(new FieldEqualityTest())
			.name("TEST 9");

		DataSet<Tuple8<Integer, String, String, Integer, String, Double, String, String>> customerFields = testEquality.map(new FilterCustomerFields());

		// Save test results to disk
		customerFields.writeAsCsv(outputTableDirectory + "/Test9.tbl", "\n", "|");

		// END: TEST 9

		// BEGIN: TEST 10 - Usage of BulkIterations and Broadcast Variables

		// pick the first price for use as highest price
		DataSet<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> firstPrice = ordersSource.reduce(new PickOneOrderRecord());

		// the partial solution is the record with the currently highest found total price
		// the total price field in the partial solution increases from iteration step to iteration step until it converges
		IterativeDataSet<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> initial = firstPrice.iterate(maxBulkIterations);

		// begin of iteration step

		// Determine the higher price
		DataSet<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> higherPrice = ordersSource.reduceGroup(new TakeFirstHigherPrice())
			.withBroadcastSet(initial, "currently_highest_price");

		DataSet<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> highestPrice = initial.closeWith(higherPrice);

		// determine maximum total price
		DataSet<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> orderWithMaxPrice = ordersSource.aggregate(Aggregations.MAX, 3);

		DataSet<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> testOrderIdentity4 = highestPrice.coGroup(orderWithMaxPrice)
			.where(3).equalTo(3).with(new CoGroupTestIdentity9())
			.name("TEST 10");

		// Save the order keys in file
		testOrderIdentity4.project(3).types(Double.class)
			.writeAsCsv(outputTableDirectory + "/Test10.tbl", "\n", "|");
		// END: TEST 10

		env.setDegreeOfParallelism(num);
		JobExecutionResult result = env.execute();

		PrintWriter out = new PrintWriter(outputAccumulatorsPath);
		out.println(result.getAccumulatorResult("count-america-customers"));
		out.println(result.getAccumulatorResult("count-europe-customers"));
		out.println(result.getAccumulatorResult("count-rest-customers"));
		out.close();

		// BEGIN: TEST 8 - only for DOP 1
		if (env.getDegreeOfParallelism() == 1) {
			int counter = result.getAccumulatorResult("count-rest-customers");
			Scanner scanner = new Scanner(new File(outputKeylessReducerPath));
			int counter2 = scanner.nextInt();
			scanner.close();

			if (counter != counter2)
				throw new Exception("TEST 8 FAILED: Keyless Reducer and Accumulator count different");
		}
		// END: TEST 8
	}

	// Joins the fields of two DataSets into one DataSet
	public static final class JoinFields1 extends JoinFunction<Tuple8<Integer, String, String, Integer, String, Double, String, String>, Tuple4<Integer, String, Integer, String>,
			Tuple12<Integer, String, String, Integer, String, Double, String, String,Integer, String, Integer, String>> {

		@Override
		public Tuple12<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String> join(Tuple8<Integer, String, String, Integer, String, Double, String, String> input1, Tuple4<Integer, String, Integer, String> input2) {
			return new Tuple12<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String>(input1.f0, input1.f1, input1.f2, input1.f3, input1.f4, input1.f5, input1.f6, input1.f7, input2.f0, input2.f1, input2.f2, input2.f3);
		}
	}

	// Joins the fields of two DataSets into one DataSet
	public static final class JoinFields2 extends JoinFunction<Tuple12<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String>, Tuple3<Integer, String, String>,
		Tuple15<Integer, String, String, Integer, String, Double, String, String,Integer, String, Integer, String, Integer, String, String>> {

		@Override
		public Tuple15<Integer, String, String, Integer, String, Double, String, String,Integer, String, Integer, String, Integer, String, String> join(Tuple12<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String> input1, Tuple3<Integer, String, String> input2) {
			return new Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>(input1.f0, input1.f1, input1.f2, input1.f3, input1.f4, input1.f5, input1.f6, input1.f7, input1.f8, input1.f9, input1.f10, input1.f11, input2.f0, input2.f1, input2.f2);
		}
	}

	// Filter for region "AMERICA" and "EUROPE"
	public static class FilterRegion extends FilterFunction<Tuple15<Integer, String, String, Integer, String, Double, String, String,Integer, String, Integer, String, Integer, String, String>> {

		private IntCounter numLines = new IntCounter();
		final String regionName;

		public FilterRegion(String rN) {
			this.regionName = rN;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator("count-"+ this.regionName.toLowerCase() + "-customers", this.numLines);
		}

		@Override
		public boolean filter(Tuple15<Integer, String, String, Integer, String, Double, String, String,Integer, String, Integer, String, Integer, String, String> input) {
			if (input.f13.equals(regionName)) {
				this.numLines.add(1);
				return true;
			} else {
				return false;
			}
		}

	}

	// Filter for regions other than "AMERICA" and "EUROPE"
	public static class FilterRegionOthers extends FilterFunction<Tuple15<Integer, String, String, Integer, String, Double, String, String,Integer, String, Integer, String, Integer, String, String>> {

		private IntCounter numLines = new IntCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator("count-rest-customers", this.numLines);
		}

		@Override
		public boolean filter(Tuple15<Integer, String, String, Integer, String, Double, String, String,Integer, String, Integer, String, Integer, String, String> input) {
			if (!input.f13.equals("AMERICA") && !input.f13.equals("EUROPE")) {
				this.numLines.add(1);
				return true;
			} else {
				return false;
			}
		}

	}

	// Extract customer fields out of customer-nation-region record
	public static class FilterCustomerFields extends MapFunction<Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>,
		Tuple8<Integer, String, String, Integer, String, Double, String, String>> {

		@Override
		public Tuple8<Integer, String, String, Integer, String, Double, String, String> map(Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String> input) throws Exception {
			return new Tuple8<Integer, String, String, Integer, String, Double, String, String>(input.f0, input.f1, input.f2, input.f3, input.f4, input.f5, input.f6, input.f7);
		}

	}

	// Test if each key has an equivalent key
	public static class CoGroupTestIdentity1 extends CoGroupFunction<Tuple1<Integer>, Tuple1<Integer>, Tuple1<Integer>> {
		@Override
		public void coGroup(Iterator<Tuple1<Integer>> input1, Iterator<Tuple1<Integer>> input2, Collector<Tuple1<Integer>> out) throws Exception {

			int count1 = 0;
			while (input1.hasNext()) {
				count1++;
				input1.next();
			}

			int count2 = 0;
			Tuple1<Integer> lastT2 = null;
			while (input2.hasNext()) {
				lastT2 = input2.next();
				count2++;
			}

			if (count1 != 1 || count2 != 1) {
				throw new Exception("TEST FAILED: The count of the two inputs do not match: " + count1 + " / " + count2 + lastT2.f0);
			}
			out.collect(lastT2);
		}
	}



	// Test if each key has an equivalent key
	public static class CoGroupTestIdentity2 extends CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {
		@Override
		public void coGroup(Iterator<Tuple2<String, Integer>> input1, Iterator<Tuple2<String, Integer>> input2, Collector<Tuple2<String, Integer>> out) throws Exception {
			int count1 = 0;
			while (input1.hasNext()) {
				count1++;
				input1.next();
			}
			int count2 = 0;
			Tuple2<String, Integer> lastT2 = null;
			while (input2.hasNext()) {
				lastT2 = input2.next();
				count2++;
			}
			if (count1 != 1 || count2 != 1) {
				throw new Exception("TEST FAILED: The count of the two inputs do not match: " + count1 + " / " + count2);
			}
			out.collect(lastT2);
		}
	}

	// Test if each key has an equivalent key
	public static class CoGroupTestIdentity8 extends CoGroupFunction<Tuple8<Integer, String, String, Integer, String, Double, String, String>, Tuple8<Integer, String, String, Integer, String, Double, String, String>, Tuple8<Integer, String, String, Integer, String, Double, String, String>> {

		@Override
		public void coGroup(Iterator<Tuple8<Integer, String, String, Integer, String, Double, String, String>> input1, Iterator<Tuple8<Integer, String, String, Integer, String, Double, String, String>> input2, Collector<Tuple8<Integer, String, String, Integer, String, Double, String, String>> out) throws Exception {

			int count1 = 0;
			while (input1.hasNext()) {
				count1++;
				input1.next();
			}

			int count2 = 0;
			Tuple8<Integer, String, String, Integer, String, Double, String, String> lastT2 = null;
			while (input2.hasNext()) {
				lastT2 = input2.next();
				count2++;
			}

			if (count1 != 1 || count2 != 1) {
				throw new Exception("TEST FAILED: The count of the two inputs do not match: " + count1 + " / " + count2);
			}
			out.collect(lastT2);
		}

	}
	
	// Test if each key has an equivalent key
	public static class CoGroupTestIdentity9 extends CoGroupFunction<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>
		, Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>, Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> {
		@Override
		public void coGroup(Iterator<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> input1, Iterator<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> input2, Collector<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> out) throws Exception {

			int count1 = 0;
			while (input1.hasNext()) {
				count1++;
				input1.next();
			}

			int count2 = 0;
			Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String> lastT2 = null;
			while (input2.hasNext()) {
				lastT2 = input2.next();
				count2++;
			}

			if (count1 != 1 || count2 != 1) {
				throw new Exception("TEST FAILED: The count of the two inputs do not match: " + count1 + " / " + count2);
			}
			out.collect(lastT2);
		}
	}

	// Crosses two input streams and returns tuple with merged fields
	public static class CrossJoinFields extends CrossFunction<Tuple16<Integer, Integer, Integer, Integer, Integer,Float, Float, Float, String, String, String, String, String, String, String, String>,
		Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>, Tuple25<Integer, Integer, Integer, Integer, Integer,Float, Float, Float, String, String, String, String, String, String, String, String, Integer, Integer, String, Double, String, String, String, Integer, String>> {

		@Override
		public Tuple25<Integer, Integer, Integer, Integer, Integer,Float, Float, Float, String, String, String, String, String, String, String, String, Integer, Integer, String, Double, String, String, String, Integer, String> cross(Tuple16<Integer, Integer, Integer, Integer, Integer,Float, Float, Float, String, String, String, String, String, String, String, String> input1, Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String> input2) throws Exception {
			return new Tuple25<Integer, Integer, Integer, Integer, Integer,Float, Float, Float, String, String, String, String, String, String, String, String, Integer, Integer, String, Double, String, String, String, Integer, String>(input1.f0, input1.f1, input1.f2, input1.f3, input1.f4, input1.f5, input1.f6, input1.f7, input1.f8, input1.f8, input1.f10, input1.f11, input1.f12, input1.f13, input1.f14, input1.f15, input2.f0, input2.f1, input2.f2, input2.f3, input2.f4, input2.f5, input2.f6, input2.f7, input2.f8);
		}

	}

	// Filters the customer key from the LineItem-Order records
	public static class FilterCustomerKeyFromLineItemsOrders extends FilterFunction<Tuple25<Integer, Integer, Integer, Integer, Integer,Float, Float, Float, String, String, String, String, String, String, String, String, Integer, Integer, String, Double, String, String, String, Integer, String>> {

		@Override
		public boolean filter(Tuple25<Integer, Integer, Integer, Integer, Integer,Float, Float, Float, String, String, String, String, String, String, String, String, Integer, Integer, String, Double, String, String, String, Integer, String> input) throws Exception {
			return (input.f0.compareTo(input.f16) == 0);
		}

	}

	// Counts the input records
	public static class ReduceCounter extends GroupReduceFunction<Tuple1<Integer>, Tuple1<Integer>> {

		@Override
		public void reduce(Iterator<Tuple1<Integer>> input, Collector<Tuple1<Integer>> out) throws Exception {
			int counter = 0;

			while (input.hasNext()) {
				input.next();
				counter++;
			}
			out.collect(new Tuple1<Integer>(counter));
		}

	}

	// Join LineItems with Orders, collect all customer keys with orders
	// (records from LineItems are not used, result contains duplicates)
	public static class CollectCustomerKeysWithOrders extends JoinFunction<Tuple16<Integer, Integer, Integer, Integer, Integer,Float, Float, Float, String, String, String, String, String, String, String, String>,
		Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>, Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> join(Tuple16<Integer, Integer, Integer, Integer, Integer,Float, Float, Float, String, String, String, String, String, String, String, String> input1, Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>input2) throws Exception {
			return new Tuple1<Integer>(input2.f1);
		}

	}

	// Removes duplicate keys
	public static class RemoveDuplicates extends ReduceFunction<Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> reduce(Tuple1<Integer> input1, Tuple1<Integer> input2) {
			return input2;
		}
	}

	// Adds a flag field to each record.
	public static class AddFlag extends MapFunction<Tuple8<Integer, String, String, Integer, String, Double, String, String>,
		Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> {

		@Override
		public Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean> map(Tuple8<Integer, String, String, Integer, String, Double, String, String> input) throws Exception {
			return new Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>(input.f0, input.f1, input.f2, input.f3, input.f4, input.f5, input.f6, input.f7, false);
		}

	}

	// Sets the last (Boolean) flag to "true".
	public static class SetFlag extends MapFunction<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>,
		Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> {

		@Override
		public Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean> map(Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean> input) throws Exception {
			return new Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>(input.f0, input.f1, input.f2, input.f3, input.f4, input.f5, input.f6, input.f7, true);
		}

	}

	// Join which directly outputs the Workset (only necessary to fulfill iteration constraints)
	public static class WorkSolutionSetJoin extends JoinFunction<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>, Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>,
		Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> {

		@Override
		public Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean> join(Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean> workSet, Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean> solutionSet) throws Exception {
			return workSet;
		}

	}

	// Outputs the first record of the input stream
	public static class PickOneRecord extends ReduceFunction<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> {

		@Override
		public Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean> reduce(Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean> input1, Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean> input2) throws Exception {
			return input2;
		}

	}

	// Outputs the first record of the input stream
	public static class PickOneOrderRecord extends ReduceFunction<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> {

		@Override
		public Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String> reduce(Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String> input1, Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String> input2) throws Exception {
			return input2;
		}

	}

	// Returns only Customers that have no matching Order
	public static class CustomersWithNoOrders extends CoGroupFunction<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>,
		Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>, Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> {

		@Override
		public void coGroup(Iterator<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> input1, Iterator<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> input2, Collector<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> out) throws Exception {

			// if no order is present output customer
			if (input1.hasNext() && !input2.hasNext()) {
				out.collect(input1.next());
			}
		}
	}

	// Only return customers that are not in input2
	public static class RemoveCheckedCustomer extends CoGroupFunction<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>,
		Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>, Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> {

		@Override
		public void coGroup(Iterator<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> workingSet, Iterator<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> checkedCustomer, Collector<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> out) throws Exception {
			if (!checkedCustomer.hasNext()) {
				while (workingSet.hasNext())
					out.collect(workingSet.next());
			}
		}
	}

	// Returns all customers with set flag
	public static class FilterFlaggedCustomers extends FilterFunction<Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean>> {

		@Override
		public boolean filter(Tuple9<Integer, String, String, Integer, String, Double, String, String, Boolean> input) throws Exception {
			return input.f8;
		}

	}

	// Gets all order keys of a customer key
	public static class OrderKeysFromCustomerKeys extends JoinFunction<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>,
		Tuple1<Integer>, Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> join(Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String> input1, Tuple1<Integer> input2) throws Exception {
			return new Tuple1<Integer>(input1.f0);
		}
	}

	// Parses the first key from a string line
	public static class ExtractKeysFromTextInput extends MapFunction<String, Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> map(String input) throws Exception {
			@SuppressWarnings("resource")
			Scanner s = new Scanner(input).useDelimiter("\\|");
			int orderKey = s.nextInt();
			s.close();
			return new Tuple1<Integer>(orderKey);
		}

	}

	// Returns only the first integer field as record
	public static class FilterFirstFieldIntKey extends MapFunction<Order, Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> map(Order input) throws Exception {
			return new Tuple1<Integer>(input.getOOrderkey());
		}
	}

	// Sum up all date counts
	public static class SumUpDateCounts extends ReduceFunction<Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> input1, Tuple2<String, Integer> input2) throws Exception {
			return new Tuple2<String, Integer>(input2.f0, input2.f1 + input1.f1);
		}
	}

	// Creates string/integer pairs of order dates
	public static class AvroOrderDateCountMap extends MapFunction<Order, Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> map(Order input) throws Exception {
			return new Tuple2<String, Integer>(input.getOOrderdate().toString(), 1);
		}

	}

	// Creates string/integer pairs of order dates
	public static class OrderDateCountMap extends MapFunction<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>, Tuple2<String, Integer>> {

		@Override
		public Tuple2<String, Integer> map(Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String> input) throws Exception {
			return new Tuple2<String, Integer>(input.f4, 1);
		}

	}

	// Joins customer with a nation records
	public static class BroadcastJoinNation extends FlatMapFunction<Tuple8<Integer, String, String, Integer, String, Double, String, String>,
		Tuple12<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String>> {

		private Collection<Tuple4<Integer, String, Integer, String>> nations;
		@Override
		public void open(Configuration parameters) throws Exception {
			nations = getRuntimeContext().getBroadcastVariable("nation");
		}

		@Override
		public void flatMap(Tuple8<Integer, String, String, Integer, String, Double, String, String> customer, Collector<Tuple12<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String>> out) throws Exception {

			for (Tuple4<Integer, String, Integer, String> nation : nations) {
				int nationKey = nation.f0;
				int customerNationKey = customer.f3;

				if (nationKey == customerNationKey) {
					out.collect(new Tuple12<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String>(customer.f0, customer.f1, customer.f2, customer.f3, customer.f4, customer.f5, customer.f6, customer.f7, nation.f0, nation.f1, nation.f2, nation.f3));
				}
			}

		}
	}

	// Joins customer-nation with a region records
	public static class BroadcastJoinRegion extends FlatMapFunction<Tuple12<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String>,
		Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>> {

		private Collection<Tuple3<Integer, String, String>> regions;
		@Override
		public void open(Configuration parameters) throws Exception {
			regions = getRuntimeContext().getBroadcastVariable("region");
		}

		@Override
		public void flatMap(Tuple12<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String> customerNation, Collector<Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>> out) throws Exception {

			for (Tuple3<Integer, String, String> region : regions) {
				int regionKey = region.f0;
				int customerNationRegionKey = customerNation.f10;

				if (regionKey == customerNationRegionKey) {
					out.collect(new Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>(customerNation.f0, customerNation.f1, customerNation.f2, customerNation.f3, customerNation.f4, customerNation.f5, customerNation.f6, customerNation.f7, customerNation.f8, customerNation.f9, customerNation.f10, customerNation.f11, region.f0, region.f1, region.f2));
				}
			}

		}
	}

	// Checks the equality of some fields of customer-nation-region record
	public static class FieldEqualityTest extends CoGroupFunction<Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>,
		Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>, Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>> {

		@Override
		public void coGroup(Iterator<Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>> input1, Iterator<Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>> input2, Collector<Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String>> out) throws Exception {
			Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String> r1 = null;
			Tuple15<Integer, String, String, Integer, String, Double, String, String, Integer, String, Integer, String, Integer, String, String> r2 = null;

			boolean failed = false;
			while (input1.hasNext() && input2.hasNext()) {
				r1 = input1.next();
				r2 = input2.next();

				// check customer name equality
				if (!r1.f1.equals(r2.f1)) {
					failed = true;
				}

				// check nation name equality
				if (!r1.f9.equals(r2.f9)) {
					failed = true;
				}

				// check region name equality
				if (!r1.f13.equals(r2.f13)) {
					failed = true;
				}

				out.collect(r2);
			}
			if (input1.hasNext() != input2.hasNext())
				failed = true;

			if (failed)
				throw new Exception("TEST FAILED: The records seem not to be equal.");
		}
	}

	public static class TakeFirstHigherPrice extends GroupReduceFunction<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>,
		Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> {

		private Collection<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> vars;
		@Override
		public void open(Configuration parameters) throws Exception {
			vars = getRuntimeContext().getBroadcastVariable("currently_highest_price");
		}
		@Override
		public void reduce(Iterator<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> input, Collector<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> out) throws Exception {

			Iterator<Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String>> iterator = vars.iterator();

			// Prevent bug in Iteration maxIteration+1
			if(!iterator.hasNext()) {
				return;
			}
			Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String> currHighestRecord = iterator.next();

			double currHighest = currHighestRecord.f3;

			Tuple9<Integer, Integer, String, Double, String, String, String, Integer, String> i = null;
			boolean collected = false;
			while (input.hasNext()) {
				i = input.next();
				double totalPrice = i.f3;

				if (totalPrice > currHighest) {
					out.collect(i);
					collected = true;
					break;
				}
			}
			if (!collected) {
				out.collect(currHighestRecord);
			}
		}

	}

	public static String getDescription() {
		return "Parameters: [customer] [lineitem] [nation] [orders] [region] [orderAvroFile] [outputTableDirectory] [maxBulkIteration] [outputAccumulator] [outputKeylessReducerPath] [numTasks]";
	}
}
