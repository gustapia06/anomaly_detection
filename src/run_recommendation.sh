#!/bin/bash

echo ""
echo ""
echo " *******************************************"
echo " *   Insight Data Science Code Challenge   *"
echo " *                                         *"
echo " *  Prepared by Gustavo Tapia              *"
echo " *  Date: 6/30/2017                        *"
echo " *                                         *"
echo " *******************************************"
echo ""
echo "To successfully run the program, the system needs to have:"
echo " - python 3.5.1 or greater"
echo " - numpy 1.12.1 or greater"
echo " - pandas 0.19.2 or greater"
echo " - networkx 1.11 or greater"
echo ""
echo " A check in the program will be carried out to see if the required software is present"
echo ""


# define data file names
BATCH='./log_input/batch_log.json'
STREAM='./log_input/stream_log.json'
FLAGGED='./log_output/flagged_purchases.json'

# check if files are in place
for FILE in $BATCH $STREAM
do
    if [[ ! -f $FILE ]]
    then
        echo "File $FILE does not exist. Aborting execution"
        exit 1
    fi
done

# want to start from a new flagged_log file, so we'll delete any existing one
if [[ -f $FLAGGED ]]
then
    rm $FLAGGED
fi
# create file
touch $FLAGGED


# run the python script
PYTHON_CMD='python3'
read -p "Warning: current python command is set to '$PYTHON_CMD'. If your python command is different please input it now. If it is the same, just press enter: " -t 7 PYTHON_CMD
if [[ "$PYTHON_CMD" = '' ]]
then
PYTHON_CMD='python3'
fi
echo "Using $PYTHON_CMD"
echo ""
$PYTHON_CMD ./src/process_data.py $BATCH $STREAM $FLAGGED
