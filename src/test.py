# Insight Data Challenge - Anomaly Detection
# Gustavo Tapia

# load needed packages
import networkx as nx
import numpy as np
import pandas as pd
import timeit

start_time = timeit.default_timer()
#######
# set corresponding types of data
def setDtypes(data):
    if isinstance(data,pd.core.frame.DataFrame):
        data = data.fillna(value=0)
        data['id'] = data['id'].astype('int32')
        data['id1'] = data['id1'].astype('int32')
        data['id2'] = data['id2'].astype('int32')
        data['amount'] = data['amount'].astype('float32')
    elif isinstance(data,pd.core.series.Series):
        data = data.fillna(value=0)
        data['id'] = np.int32(data['id'])
        data['id1'] = np.int32(data['id1'])
        data['id2'] = np.int32(data['id2'])
        data['amount'] = np.float32(data['amount'])
    
    return data


# get the D-degree social network for user id from graph
def getDnetwork(graph,id,D):
    Dneighbors = nx.single_source_shortest_path_length(graph, id, cutoff=D)
    return set(Dneighbors.keys()).difference({id})



# get the T latest purchases from D social network of user id
def getTpurchases(data,IDnetwork,T):
    # get those purchases made from the network. They will be sorted in oldest to latest so only get those
    #   last T purchases
    lastTpurchases = data.loc[data['id'].isin(IDnetwork),'amount'].tail(T)
    return lastTpurchases



# analyze the actual purchase amount and see if it should be flagged or not based on last T
#   purchases from the ID network
def analyzePurchase(network_purchases,event):
    # check if number of purchases from social network is enough (>2)
    if network_purchases.shape[0] >= 2:
        mean_purchases = network_purchases.mean()
        std_purchases = network_purchases.std()
        
        # check if it should be flagged or not
        if event['amount'] > (mean_purchases + 3*std_purchases):
            flagPurchase(event,mean_purchases,std_purchases)
#            print('Purchase flagged')



def flagPurchase(event,mean_val,std_val):
    vals = pd.Series({'mean':round(mean_val,2),'sd':round(std_val,2)})
    event_json = event.drop(['id1','id2']).append(vals).astype('str').to_json(double_precision=2)
    with open(flagged_data_name, 'a') as outfile:
        print(event_json, file = outfile)



#########

#def main():
#################
# file names
global hist_data_name
global stream_data_name
global flagged_data_name
hist_data_name = './log_input/batch_log.json'
stream_data_name = './log_input/stream_log.json'
flagged_data_name = './log_output/flagged_purchases.json'

#################
# load historically data from ./log_input/batch_log.json
hist_data = pd.read_json(hist_data_name, lines=True, convert_dates = True)

# get the two parameters T and D. Make sure they are ints
T = int(hist_data.loc[0,'T'])
D = int(hist_data.loc[0,'D'])

# remove first two columns and first row that are not needed anymore
colnames = ['event_type','timestamp','id','id1','id2','amount']
hist_data = hist_data.loc[1:,colnames]


###################
# perform checks of historical data, clean and redefine if needed
# event_type can only be purchase, befriend, unfriend. See if empty values present
event_types = {'befriend', 'purchase', 'unfriend'}
if set(hist_data['event_type'].unique()) != event_types:
    print('Event types not identified are present')
    hist_data = hist_data.drop(hist_data.index[~hist_data.event_type.isin(event_types)])


# check for each event_type and drop those rows according to:
#   when purchase: amount and id cannot be nan
hist_data = hist_data.drop(hist_data.index[(hist_data['event_type']=='purchase') & ((np.isnan(hist_data['amount'])) | (np.isnan(hist_data['id'])))])

#   when be/unfriend: id1 and id2 cannot be nan
hist_data = hist_data.drop(hist_data.index[(hist_data['event_type'].isin(event_types.difference({'purchase'}))) & ((np.isnan(hist_data['id1'])) | (np.isnan(hist_data['id2'])))])

# change nan for 0 and change data types to integers and floats with less precision (not needed float64)
hist_data = setDtypes(hist_data)

##################
#set up the social network graph for Market-ter based on how the records
# were registered in the json file
MKTT = nx.Graph()

# iterate through them all since two nodes can befriend, unfriend and befriend again
# also, iterate only through those that have friendships (there are some users that might have
#   no frieds but still have purchases) since the recommendation system is based on friendship
#   only and not similarity
for idx,event in hist_data.loc[hist_data['event_type'] != 'purchase'].iterrows():
    if event['event_type'] == 'befriend':
        MKTT.add_edge(event['id1'],event['id2'])
    elif event['event_type'] == 'unfriend':
        # add a exception in case we are trying to remove a non-existent friendship
        try:
            MKTT.remove_edge(event['id1'],event['id2'])
        except nx.exception.NetworkXError:
            print('Trying to remove a non-existent edge')
            pass

##################
# drop unnecessary friedship information since the graph has been created already
hist_data = hist_data.drop(hist_data.index[(hist_data['event_type'].isin({'befriend','unfriend'}))])

##################
# once the network graph is created, start the stream of new data
# load historically data from ./log_input/batch_log.json
stream_data = pd.read_json(stream_data_name, lines=True, convert_dates = True)

# since this is suppose to simulate incoming data from an API, not prior cleaning for the whole
#   'streaming' dataframe will be done. Instead, at each iteration, it will check for validity of data
for idx,event in stream_data.iterrows():
    # check for type of event
    if event['event_type'] == 'befriend':
        if np.isnan(event['id1']) or np.isnan(event['id2']):
            #then not valid values, continue to next row
            continue
        else:
            event_temp = setDtypes(event)
            MKTT.add_edge(event_temp['id1'],event_temp['id2'])

    elif event['event_type'] == 'unfriend':
        if np.isnan(event['id1']) or np.isnan(event['id2']):
            #then not valid values, continue to next row
            continue
        else:
            event_temp = setDtypes(event)
            # add a exception in case we are trying to remove a non-existent friendship
            try:
                MKTT.remove_edge(event_temp['id1'],event_temp['id2'])
            except nx.exception.NetworkXError:
#                print('Trying to remove a non-existent edge')
                pass

    elif event['event_type'] == 'purchase':
        if np.isnan(event['id']) or np.isnan(event['amount']):
            #then not valid values, continue to next row
            continue
        
        # convert data types
        event_temp = setDtypes(event)

        # check if id of current purchase is in the network (has friends?). If no friends, this
        #   recommendation algorithm don't deal with that id at all.
        if event_temp['id'] in set(MKTT.nodes()):
            Dnetwork = getDnetwork(MKTT,event_temp['id'],D)
            Tpurchases = getTpurchases(hist_data,Dnetwork,T)
            analyzePurchase(Tpurchases,event_temp)

            # add last 'purchase' event to historical dataset, again no need to save friendship
            #   history right now since it has already been updated in the graph
            hist_data = hist_data.append(event_temp,ignore_index=True)

elapsed = timeit.default_timer() - start_time
print(elapsed)


#if __name__ == "__main__":
#    main()
