echo "Updating configuration. Remember to restart"

. ./configDefaults.sh

for file in str-conf/* ; do
	filename=$(basename "$file")
	extension="${filename##*.}"
	if [[ "$extension" != "template" ]]; then
		echo "copying $file to $FILES_DIRECTORY/stratosphere-build/conf/"
		cp $file $FILES_DIRECTORY/stratosphere-build/conf/
	fi
done
