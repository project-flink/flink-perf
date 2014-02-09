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
java -cp target/testjob-0.1-SNAPSHOT.jar eu.stratosphere.test.testPlan.SequenceFileGenerator SeqOut 1000000 15
```
(45 MB)


## Execute Plan

