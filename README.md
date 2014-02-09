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

2. Generate Sequencefile

```
java -cp <path to JAR>.jar eu.stratosphere.test.testPlan.SequenceFileGenerator outFIle 10000 15
<path to JAR> is usually target/
```


## Execute Plan

