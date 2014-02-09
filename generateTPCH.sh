echo "Generating test data. Expecting to be in root directory"

# call prepareTPCH.sh before

cd tpch_2_16_0/dbgen

./dbgen

mkdir data
mv *.tbl data/

echo "find the generated data in tpch_2_16_0/dbgen/data"