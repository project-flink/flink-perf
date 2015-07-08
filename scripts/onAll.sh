echo "Executing $@ everywhere"

for i in $(seq 0 39);
do
	ssh -n "robert-streaming-w-$i" -- $@
done


