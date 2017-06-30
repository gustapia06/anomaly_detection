#!/bin/bash


echo "To successfully run the program, the system needs to have:"
echo " - python 3.5.1 or greater"
echo " - numpy 1.12.1 or greater"
echo " - pandas 0.19.2 or greater"
echo " - networkx 1.11 or greater"
echo ""
echo " A check in the program will be carried out to see if the required software is present"
echo ""
echo ""


# define file names
BATCH='./log_input/batch_log.json'
STREAM='./log_input/stream_log.json'
FLAGGED='./log_output/flagged_purchases.json'

# check files are in place
for FILE in $BATCH $STREAM
do
    if [ ! -f $FILE ]
    then
        echo "File $FILE exist. Aborting execution"
        exit 0
    fi
done

# want to start from a new flagged_log file, so we'll delete any existing one
if [ -f $FLAGGED ]
then
    rm $FLAGGED
fi
# create file
touch $FLAGGED


# run the python script
python3 ./src/process_data.py $BATCH $STREAM $FLAGGED
