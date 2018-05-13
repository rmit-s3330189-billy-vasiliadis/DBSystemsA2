#!/bin/bash
if [ "$1" != '' ]
then

	echo "removing header from csv"
	tail -n +2 "$1" > BUSINESS_NAMES_201803.csv.nohead
	echo "compiling code"
	javac *.java
	echo -e "loading heap file\n"
	java dbload -p 4096 BUSINESS_NAMES_201803.csv.nohead
	echo "querying heap for \"mf engineering\""
	java dbquery "mf engineering" 4096
else
	echo "Usage: bash ./part3.sh input_csv_file"
fi
