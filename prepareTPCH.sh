echo "Preparing TPHC"

wget http://www.tpc.org/tpch/spec/tpch_2_16_0.zip
unzip tpch_2_16_0.zip
cd tpch_2_16_0
cd dbgen
mv makefile.suite Makefile

# set Makefile configuration values
sed -i 's/CC      =/CC      = cc/g' Makefile
sed -i 's/DATABASE=/DATABASE= SQLSERVER/g' Makefile
sed -i 's/MACHINE =/MACHINE = LINUX/g' Makefile
sed -i 's/WORKLOAD =/WORKLOAD = TPCH/g' Makefile

make
