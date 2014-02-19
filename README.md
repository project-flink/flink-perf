testjob
=======

Contains (for now) one large test job that tests all Stratosphere components.

## Background information

Original issue for planning this feature: https://github.com/stratosphere/stratosphere/issues/379



## Preparation

### Generate TPC-H Data

Download and build the TPC-H generate tool using `./prepareTPCH.sh`.

Generate data using `./generateTPCH.sh`

### Generate Special Data

1. Generate Avro File(s)

This command will generate an Avro file from the original TPC-H orders file. (This step is not necessary when running the test job locally.)

```
java -cp target/testjob-0.1-SNAPSHOT.jar eu.stratosphere.test.testPlan.LargeTestPlan '/input/orders.tbl' '/output/orders.avro'
```

2. Generate Sequencefile

```
java -cp target/testjob-0.1-SNAPSHOT.jar eu.stratosphere.test.testPlan.SequenceFileGenerator SeqOut 1000000 15
```
(45 MB)


## Execute Plan

### Execute Plan with LocalExecutor

The following parameters need to be passed to the main()-Method of LargeTestPlan (in that order).

| Parameter                | Description                                                            | Example                    |
| ------------------------ | ---------------------------------------------------------------------- | -------------------------- |
| customer                 | path to TPC-H file                                                     | file:///.../customer.tbl   |  
| lineitem                 | path to TPC-H file                                                     | file:///.../lineitem.tbl   |
| nation                   | path to TPC-H file                                                     | file:///.../nation.tbl     |
| orders                   | path to TPC-H file                                                     | file:///.../orders.tbl     |
| region                   | path to TPC-H file                                                     | file:///.../region.tbl     |
| orderAvroFile            | path to generated Avro file                                            | file:///.../orders.avro    |
| sequenceFileInput        | path to generated Sequence file                                        | file:///.../seqfile        |
| outputTableDirectory     | path to test output directory                                          | file:///.../directory/     |
| maxBulkIterations        | max bulk iterations (Test 10), should be greater than number of orders | 1000                       |
| ordersPath               | ordinary path to TPC-H order file                                      | /.../orders.tbl            |
| outputAccumulatorsPath   | ordinary path where accumulator results get stored                     | /.../accumulators.txt      |
| outputKeylessReducerPath | ordinary path where AllReduce count results get stored                 | /.../allreduce.txt         |
| outputOrderAvroPath      | ordinary path where the generated Avro file gets stored                | /.../orders.avro           |

### Execute Plan on Cluster

The following parameters need to be passed to the Job (in that order). The Avro file and the Sequence file need to be generated previously.

| Parameter                | Description                                                            | Example                    |
| ------------------------ | ---------------------------------------------------------------------- | -------------------------- |
| customer                 | path to TPC-H file                                                     | file:///.../customer.tbl   |  
| lineitem                 | path to TPC-H file                                                     | file:///.../lineitem.tbl   |
| nation                   | path to TPC-H file                                                     | file:///.../nation.tbl     |
| orders                   | path to TPC-H file                                                     | file:///.../orders.tbl     |
| region                   | path to TPC-H file                                                     | file:///.../region.tbl     |
| orderAvroFile            | path to generated Avro file                                            | file:///.../orders.avro    |
| sequenceFileInput        | path to generated Sequence file                                        | file:///.../seqfile        |
| outputTableDirectory     | path to test output directory                                          | file:///.../directory/     |
| maxBulkIterations        | max bulk iterations (Test 10), should be greater than number of orders | 1000                       |

