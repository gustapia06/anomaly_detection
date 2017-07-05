# Insight Data Challenge - Anomaly Detection
# Gustavo Tapia

#########################
# load needed packages
import networkx as nx
import numpy as np
import pandas as pd
import sys
import timeit

# Function definitions
##******************************************************
# Main function
def main():
    #########################
    # check versions for required packages
    check_versions()
    
    #########################
    # load historical data
    hist_data = pd.read_json(hist_data_name, lines=True, convert_dates = True)

    # get the two parameters T and D. Make sure they are ints
    try:
        T = int(hist_data.loc[0,'T'])
        D = int(hist_data.loc[0,'D'])
    except:
        raise ExecError('Error: No D or T values found. Execution aborted')
    
    #########################
    # clean historical data
    hist_data = clean_hist_data(hist_data)

    #########################
    #set up the social network graph for Market-ter based on how the records
    # were registered in the historical json file
    MKTT = nx.Graph()

    # iterate through them all sequentially since two nodes can befriend, unfriend and befriend again.
    #   Also, iterate ONLY through those that have friendships since the recommendation system is based
    #   on friendship only and not similarity (there might be some users that have no friends but still
    #   have purchases)
    for idx,event in hist_data.loc[hist_data['event_type'] != 'purchase'].iterrows():
        if event['event_type'] == 'befriend':
            MKTT.add_edge(event['id1'],event['id2'])
        elif event['event_type'] == 'unfriend':
            # add a exception in case we are trying to remove a non-existent friendship
            try:
                MKTT.remove_edge(event['id1'],event['id2'])
            except:
                #print('Trying to remove a non-existent edge')
                pass

    #########################
    # drop unnecessary friedship information since the graph has been created already
    hist_data = hist_data.drop(hist_data.index[(hist_data['event_type'].isin({'befriend','unfriend'}))])

    #########################
    # once the network graph is created, start the stream of new data
    # load stream data
    stream_data = pd.read_json(stream_data_name, lines=True, convert_dates = True)

    # start timer for statistic purposes
    time_start = timeit.default_timer()
    
    # since this is suppose to simulate incoming data from an API, no prior cleaning will be done (as with
    #   historical data). Instead, at each iteration, the code will check for validity of data
    for idx,event in stream_data.iterrows():
        # check for type of event
        if event['event_type'] == 'befriend':
            if np.isnan(event['id1']) or np.isnan(event['id2']):
                #then not valid values, continue to next row
                continue
            else:
                # if valid data, updates data types and update the graph
                event_temp = setDtypes(event)
                MKTT.add_edge(event_temp['id1'],event_temp['id2'])

        elif event['event_type'] == 'unfriend':
            if np.isnan(event['id1']) or np.isnan(event['id2']):
                #then not valid values, continue to next row
                continue
            else:
                # if valid data, updates data types and update the graph
                event_temp = setDtypes(event)
                # add a exception in case we are trying to remove a non-existent friendship
                try:
                    MKTT.remove_edge(event_temp['id1'],event_temp['id2'])
                except:
                    pass

        elif event['event_type'] == 'purchase':
            if np.isnan(event['id']) or np.isnan(event['amount']):
                #then not valid values, continue to next row
                continue
            
            # convert data types
            event_temp = setDtypes(event)

            # check if id of current purchase is in the network (has friends?). If no friends, don't
            #   do anything since this recommendation algorithm only deal with those with friendships
            if (event_temp['id'] not in set(MKTT.nodes())) or (len(MKTT.edges(event_temp['id'])) == 0):
                # only add current purchase to historical dataset even if no friendship is found, since
                #   in the future there might be a friendship and we want to include this in the data
                hist_data = hist_data.append(event_temp,ignore_index=True)
                continue
            
            # get D social network for id
            Dnetwork = getDnetwork(MKTT,event_temp['id'],D)

            # get the last T purchases from the social network
            Tpurchases = getTpurchases(hist_data,Dnetwork,T)

            # analyze if current purchase should be flagged or not
            analyzePurchase(Tpurchases,event_temp)

            # add last 'purchase' event to historical dataset, again no need to save friendship
            #   history right now since it has already been updated in the graph
            hist_data = hist_data.append(event_temp,ignore_index=True)

    #########################
    # end of data analysis
    total_time = timeit.default_timer() - time_start
    print('')
    print('Stream of data finished')
    print('Total time: {:.3f} sec'.format(total_time))
    print('Total data processed: {:d} events'.format(stream_data.shape[0]))
    print('Average time for processing an event: {:.4f} sec'.format(total_time/stream_data.shape[0]))
    print('')
    print('Program successfully finished')
    print(' *******************************************')


##******************************************************
# Check versions of packages
def check_versions():
    #########################
    # ask if check needed
    usercheck = input('Do you want the code to check the versions of packages? (yes or no): ')
    if usercheck == 'no':
        return 0
    elif usercheck == 'yes':
        print('Performing version check')
    else:
        print('Answer not identified. Performing version check')
    
    #########################
    # dictionary of needed versions (py is python, np is numpy, pd is pandas, nx is networkx
    need_ver = {'py': [3,5,1], 'np': [1,12,1], 'pd': [0,19,2], 'nx': [1,11]}
    try:
        sys_ver = {'py': sys.version.split(' ')[0].split('.'), 'np': np.__version__.split('.'),
            'pd': pd.__version__.split('.'), 'nx': nx.__version__.split('.')}
    except:
        # if packages are not found
        raise ExecError('Needed packages could not be found')
    
    #########################
    # iterate over each package
    for pack in need_ver.keys():
        # iterate over from major version number to minor version number
        for i in range(len(need_ver[pack])):
            # sometimes it shows .3.4 so add that missing 0
            if sys_ver[pack][i] == '':
                sys_ver[pack][i] = '0'
            
            # if version is less, then raise an error
            if int(sys_ver[pack][i]) < need_ver[pack][i]:
                raise ExecError('Check installed versions to be able to succesfully run the code')
            elif int(sys_ver[pack][i]) > need_ver[pack][i]:
                # if major version is greater then no need to check other subversions
                break

    # check passed
    print('Version check passed')


##******************************************************
# Function to set data types
def setDtypes(data):
    #########################
    # two cases: it's all data or single event
    
    # if whole dataset
    if isinstance(data,pd.core.frame.DataFrame):
        colnames = set(data.columns.values)
        data = data.fillna(value=0)
        
        # id's are int
        for i in {'id','id1','id2'}:
            if i in colnames:
                data[i] = data[i].astype('int32')
    
        #amount is float
        if 'amount' in colnames:
            data['amount'] = data['amount'].astype('float32')
    
    # if single event
    elif isinstance(data,pd.core.series.Series):
        rownames = set(data.index.values)
        data = data.fillna(value=0)
        
        # id's are int
        for i in {'id','id1','id2'}:
            if i in rownames:
                data[i] = np.int32(data[i])

        # amount is float
        if 'amount' in rownames:
                data['amount'] = np.float32(data['amount'])

    return data


##******************************************************
# Clean the historical dataset
def clean_hist_data(data):
    #########################
    # remove first two columns and first row that are not needed anymore
    colnames = ['event_type','timestamp','id','id1','id2','amount']
    data = data.loc[1:,colnames]
    
    #########################
    # perform checks, clean and redefine data types if needed
    # event_type can only be purchase, befriend, unfriend. Drop any other types
    event_types = {'befriend', 'purchase', 'unfriend'}
    if not set(data['event_type'].unique()).issubset(event_types):
        print('Event types not identified are present')
        print(data['event_type'].unique())
        data = data.drop(data.index[~data.event_type.isin(event_types)])
    
    #########################
    # check for each event_type and drop those rows according to:
    #   if type == purchase: amount and id cannot be nan
    data = data.drop(data.index[(data['event_type']=='purchase') & ((np.isnan(data['amount'])) | (np.isnan(data['id'])))])

    #   if type == be/unfriend: id1 and id2 cannot be nan
    data = data.drop(data.index[(data['event_type'].isin(event_types.difference({'purchase'}))) & ((np.isnan(data['id1'])) | (np.isnan(data['id2'])))])
    
    #########################
    # change nan for 0 and change data types to integers and floats with less precision (not needed float64)
    data = setDtypes(data)
    return data


##******************************************************
# Function to get the D-degree social network for user id from graph
def getDnetwork(graph,id,D):
    Dneighbors = nx.single_source_shortest_path_length(graph, id, cutoff=D)
    return set(Dneighbors.keys()).difference({id})


##******************************************************
# Function to get the T latest purchases from D social network of user id
def getTpurchases(data,IDnetwork,T):
    # get those purchases made from the network. They will be sorted in oldest to latest so only get those
    #   last T purchases
    lastTpurchases = data.loc[data['id'].isin(IDnetwork),'amount'].tail(T)
    return lastTpurchases


##******************************************************
# Function to analyze the actual purchase amount and see if it should be flagged or not based on last T
#   purchases from the ID network
def analyzePurchase(network_purchases,event):
    # check if number of purchases from social network is enough (>2)
    if network_purchases.shape[0] >= 2:
        mean_purchases = network_purchases.mean()
        std_purchases = network_purchases.std(ddof=0)
        
        # check if it should be flagged or not
        if event['amount'] > (mean_purchases + 3*std_purchases):
            flagPurchase(event,mean_purchases,std_purchases)

##******************************************************
# Function to write a flagged purchase
def flagPurchase(event,mean_val,std_val):
    # format mean and std values
    vals = pd.Series({'mean':"{:.2f}".format(mean_val),'sd':"{:.2f}".format(std_val)})
    
    # merge event info and mean and std vals and write to file
    event = event[['event_type','timestamp','id','amount']]
    event_json = event.append(vals).astype('str').to_json()
    with open(flagged_data_name, 'a') as outfile:
        print(event_json, file = outfile)


##******************************************************
# Define error class ExecError for checking versions of packages
class ExecError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)


##******************************************************
# main execution
if __name__ == "__main__":
    #########################
    # assign global file names
    global hist_data_name
    global stream_data_name
    global flagged_data_name
    hist_data_name = sys.argv[1]
    stream_data_name = sys.argv[2]
    flagged_data_name = sys.argv[3]
    
    main()
