package eu.stratosphere.test.javaTestPlan;

import eu.stratosphere.test.testPlan.Order;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.util.Scanner;

public class AvroFileGenerator {

	public static String outputOrderAvroPath;
	public static String ordersPath;

	public static void main(String[] args) throws Exception {
		// generate only avro file
		if (args.length == 2) {
			ordersPath = args[0];
			outputOrderAvroPath = args[1];
			// Generate file for avro test
			DatumWriter<Order> orderDatumWriter = new SpecificDatumWriter<Order>(Order.class);
			DataFileWriter<Order> dataFileWriter = new DataFileWriter<Order>(orderDatumWriter);
			dataFileWriter.create(Order.getClassSchema(), new File(outputOrderAvroPath));
			Scanner s = new Scanner(new File(ordersPath));
			while (s.hasNextLine()) {
				@SuppressWarnings("resource")
				Scanner lineScanner = new Scanner(s.nextLine()).useDelimiter("\\|");

				Order o = new Order();
				o.setOOrderkey(lineScanner.nextInt());
				o.setOCustkey(lineScanner.nextInt());
				o.setOOrderstatus(lineScanner.next());
				o.setOTotalprice(lineScanner.nextFloat());
				o.setOOrderdate(lineScanner.next());
				o.setOOrderpriority(lineScanner.next());
				o.setOClerk(lineScanner.next());
				o.setOShipproprity(lineScanner.nextInt());
				o.setOComment(lineScanner.next());
				dataFileWriter.append(o);
				lineScanner.close();
			}
			dataFileWriter.flush();
			s.close();
			dataFileWriter.close();
			return;
		} else {
			System.err.println("Usage: <inputFilePath> <outputAvroPath>");
			System.exit(1);
		}
	}
}
