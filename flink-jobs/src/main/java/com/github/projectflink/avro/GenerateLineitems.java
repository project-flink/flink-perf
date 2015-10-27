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
package com.github.projectflink.avro;

import com.github.projectflink.generators.tpch.generators.core.DistributedTPCH;
import com.github.projectflink.generators.tpch.generators.core.TpchEntityFormatter;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.flink.api.java.ExecutionEnvironment;

public class GenerateLineitems {
	public static void main(String[] args) throws Exception {
		// Parse and handle arguments
		ArgumentParser ap = ArgumentParsers.newArgumentParser("Distributed TPCH");
		ap.defaultHelp(true);
		ap.addArgument("-s", "--scale").setDefault(1.0).help("TPC H Scale (final Size in GB)").type(Double.class);
		ap.addArgument("-p","--parallelism").setDefault(1).help("Parallelism for program").type(Integer.class);
		ap.addArgument("-e", "--extension").setDefault(".csv").help("File extension for generated files");
		ap.addArgument("-o", "--outpath").setDefault("/tmp/").help("Output directory");
		
		Namespace ns = null;
        try {
            ns = ap.parseArgs(args);
        } catch (ArgumentParserException e) {
            ap.handleError(e);
            System.exit(1);
        }
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ns.getInt("parallelism"));
		DistributedTPCH gen = new DistributedTPCH(env);
		gen.setScale(ns.getDouble("scale"));

		String base = ns.getString("outpath");
		String ext = ns.getString("extension");
		gen.generateLineItems().writeAsFormattedText(base + "lineitems" + ext, new TpchEntityFormatter());

		env.execute("Generate Lineitems, Scale = "+gen.getScale());
	}

}
