# Table of Contents
1. [Code Summary](README.md#code-summary)
2. [Requirements](README.md#requirements)
3. [Details of Implementation](README.md#details-of-implementation)
4. [Directory Structure](README.md#directory-structure)
5. [Instructions](README.md#instructions)


# Code Summary

The present project proposes a solution for the Insight Data Science - Anomaly Detection challenge. The objective is to create a recommendation system for data coming from an e-commerce and social network company that will be able to flag purchases that are deemed as important or above average, such that users can see which items their friends are buying, and be influenced by these within-network purchases. 

The requirements, details and instructions of the recommendation system are explained in the following sections, such that the user is able to run the code with no issues. 

# Requirements
The code contained in this repository was mainly developed in Python and Unix/Bash. There are two files in the `src` directory:

`run_recommendation.sh` is a shell file that will check if the required files are on place and start the python script. This shell script should be able to run in any "modern" version of Bash shell.  

`process_data.py` is a python script that will implement the recommendation system.

## Python environment requirements
The python script needs to be run in `python` version 3 (any version), however it has only been successfully tested for versions 3.5.1 or greater. Additionally few libraries are needed for the script to run successfully: `Numpy` version 1.12.1 or greater, `pandas` version 0.19.2 or greater, and `networkx` version 1.11 or greater. 

# Details of Implementation
## Main Objective
The main goal of the recommendation system is to flag as important (or above average) certain purchase events within the social network of a user, such that its social network is informed about that event and hopefully influenced to purchase in a similar behavior.

The implementation consists in reading a historical dataset that includes friendship information as well as purchase events from different users identified by a unique ID number. Furthermore, all of these events present a timestamp so it is easier to identify events historically.
Based off from this historical dataset, an incoming stream of events is to be analyzed in order to identify such important purchases.

Two different parameters are needed to set up this system:

`D` is the number of degrees that defines a user's social network, that is, how deep in the user's network are we interested on. A value of `1` means that we only consider the immediate friends of the user, whereas a value of `2` means the social network extends to friends and "friends of friends".

`T` is the number of consecutive purchases made by a user's social network (not including the user's own purchases). This set of purchases are used to deem an specific purchase as important or not. We restrict this parameter to be at least `2` to have enough valid historical information when drawing conclusions.

The recommendation system works as follows: as the incoming stream of data is analyzed, the system identifies a purchase event made by some user as anomalous only if the purchase amount is more than 3 standard deviations from the mean of the last `T` purchases in the user's `D`th degree social network. If a user's social network has less than 2 purchases, we don't have enough historical information, so no purchases are considered anomalous at that point. If a user's social network has made 2 or more purchases, but less than `T`, we still proceed with the calucations to determine if the purchases are anomalous.

## Implementation
The workflow of the implementation starts in file `run_recommendation.sh`. This file will check for the required data files: `batch_log.json` and `stream_log.json` that are to be placed in folder `log_input/` on the root of the repository. Then, it will create a new file `flagged_log.json` where flagged events will be recorded. If this file exists before execution, the code will delete its previous contents and start a new log. Lastly, the code will ask what command to use in order to execute the python script, given that some systems use `python` or `python3` depending on the installed versions. The default python command is `python3` given that at least version 3 is required.

Next, we start running file `process_data.py`. In this file, first we define the functions (including `main()`) that will be needed for different actions in the code:

`main`: is the main body of the code for the reccomendation system.

`check_versions`: checks the versions for the different packages before execution to make sure the code will run.

`setDtypes`: set different datatypes for the data read from the different files to keep consistency among them all.

`clean_hist_data`: performs different checks in the historical dataset and cleans it if missing data or errors are identified.

`getDnetwork`: gets the D-degree social network for some specific user

`getTpurchases`: gets the lastest T purchases from the D-degree social network for some specific user

`analyzePurchase`: analyze the actual purchase with respect with the last T purchases from the D-degree social network for some specific user to deem it as anomalous or not.

`flagPurchase`: flags a purchase as anomalous and writes it into the log file along with its statistics.

The workflow of function `main()` starts with check of required package versions. The user will be asked if they want this check to be performed or not. If any package is not found, or version requirements are not satisfied, execution of the code will be aborted. Next, the historical dataset is read, which includes values for parameters `T` and `D`. If these values are not found, again execution will be aborted.

Subsequently, we proceed with cleaning up the historical data by removing unneeded columns and identifying if there are missing values for important fields (i.e., if `event_type` is `purchase` then both `amount` and `id` have to be provided; similarly, if `event_type` is `befriend` or `unfriend` then both `id1` and `id2` are expected). To finish up the cleaning stage, we convert all data to specific format depending on the field (i.e., all `id`s are integers, `amount`s are floats).

Next, we create a graph for the social network using the `networkx` package. A loop iterating over every record of friendship from the historical data is done to create the graph. Once this loop finishes, we remove all of those records from the dataset (release them from memory) since the graph is up-to-date and we only need those historical purchase events from now on.

Once all the historical data has been set up, it is now the time to start the simulation of an incoming stream of new events. Since this is done in place of a real-time API, the code reads every single event and decides what to do given the information contained on it. Therefore, no previous data cleaning is done (as it was done with the historical dataset).

For every incoming event, it can only be of the type `befriend`, `unfriend` or `purchase`. If the new event is related to friendship (i.e., befriend or unfriend), then the graph is updated as needed after checking validity of the data (that is, check for fields `id1` and `id2`).
In the case that the event is a `purchase`, then the code first checks the validity of data and converts the data into the datatypes needed to be consistent with the historical data and the graph. Since the recommendation systems analyzes above-average behavior within a user's social network, another check is then executed in order to determine if the purchaser `id` is active in the social network (in case of new users) and has any friendships at the time of purchase. If not, the code doesn't lose any time doing any calculations. For those `id`s that are present in the graph, its D-degree social network is identified and the last T purchases from that network are queried. Consequently, the current event is compared with the set of T identified purchases, and if the amount is greater than 3 standard deviations from the mean, the event is flagged as important and logged in the `flagged_log.json` file.

Once all events from the streaming data have been analyzed, the code shows some statistics about its performace. Specifically, it shows the number of events analyzed, the total time taken by the program, and the average time per event that it takes to analyze. 

# Directory Structure

The directory structure for this project looks like this:

    .
    ├── README.md 
    ├── run.sh
    ├── src
    │   ├── process_data.py
    |   └── run_recommendation.sh
    ├── log_input
    │   ├── batch_log.json
    │   └── stream_log.json
    ├── log_output
    |   └── flagged_purchases.json
    └── insight_testsuite
        ├── run_tests.sh
        └── tests
            ├── test_1
            |   ├── log_input
            |   |   ├── batch_log.json
            |   |   └── stream_log.json
            |   └── log_output
            |       └── flagged_purchases.json
            ├── test_2
            |   ├── log_input
            |   |   ├── batch_log.json
            |   |   └── stream_log.json
            |   └── log_output
            |       └── flagged_purchases.json
            └── test_3
                ├── log_input
                |   ├── batch_log.json
                |   └── stream_log.json
                └── log_output
                    └── flagged_purchases.json


# Instructions

In order to successfully run the code, you will need to have the same directory structure as shown in the previous section. From a terminal window, you will only need to execute batch file `run.sh` from the directory root:
    
    anomaly_detection~$ ./run.sh

