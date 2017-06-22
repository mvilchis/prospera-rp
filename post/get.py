# -*- coding: utf-8 -*-

'''
This module sets the foundation for all get requests executed via
RapidPro's Application Programming Interface.

'''

import os
import configparser
import json
import requests
import csv
import pandas as pd
from operator import itemgetter
import pprint
import sys
from datetime import datetime
import copy
import time
############ rapidpro client ############
import dateutil.parser
from io import StringIO
import os.path
from temba_client.v2 import TembaClient
import sys
import tailer
from six import string_types

#configuration
config = configparser.ConfigParser()
config.read('keys.ini')
## Paths
#processed_runs = config['paths']['processed_runs']
root = config['paths']['root']
raw_flows = config['paths']['raw_flows']
raw_runs = config['paths']['raw_runs']
raw_contacts = config['paths']['raw_contacts']
raw_messages = config['paths']['raw_messages']
raw_fields = config['paths']['raw_fields']
raw_groups = config['paths']['raw_groups']
## Rapidpro
rp_api = config['rapidpro']['rp_api']

PRINT_PAGE = 100
MAX_RETRY_ALL = 10
PARTITION_NUMBER = 1000

class Get(object):

    '''
        Encompasses all functions related to getting RapidPro messages and
        incorporating them to our master dataset.
    '''
    def __init__(self):
        super(Get, self).__init__()
        #We dont need all flows updates in this moment
        try:
            self.df_raw_flows = pd.read_csv(root + raw_flows)
        except Exception:
            pass
        ############ rapidpro client ############d
        # rp_api format: 'Token value', TembaClient use value
        token = rp_api.split(' ')[1]
        self.client_io = TembaClient('rapidpro.io',token)

    def get_client_request(self,before=None, after = None):
        '''
            This function is to be overwritten in subclasses.
        '''
        raise Exception("No request form has been identified."
                        "Enter the appropriate subclass.")


    def flatten_dict(self, d, result = None):
        '''
            Recursively flattens a dictionary. The only requirement is that
            the dictionary does not have a list within directly contained
            in another list, e.g. {'a':1, 'b':2, 'c':[3, 2, [1, 2, 3]]} no.
            Yes: {'antes':{'a':{'i':11, 'ii':13, 'iii':{'qwer':21}}},
                    'b':2, 'c':[3, 2, {'q':21, 'r':222, 'k':[1, 2, 3]}]}
        '''

        if result is None:
            result = {}

        for key in d:
            value = d[key]

            if isinstance(value, dict):
                value1 = {}
                for keyIn in value:
                    value1["_".join([key,keyIn])]=value[keyIn]
                self.flatten_dict(value1, result)

            elif isinstance(value, (list, tuple)):
                for indexB, element in enumerate(value):

                    if isinstance(element, dict):
                        value1 = {}
                        index = 0

                        for keyIn in element:
                            newkey = "_".join([key, str(indexB),keyIn])
                            value1[newkey]=value[indexB][keyIn]
                            index += 1

                        for keyA in value1:
                            self.flatten_dict(value1, result)

                    elif isinstance(element, (list, tuple)):
                        pass

                    else:
                        newkey = "_".join([key,str(indexB)])
                        result[newkey] = element

            else:
                result[key]=value

        return result


    def to_df(self,result_list):
        '''
            Runs a request, extracts messages and assembles them.
        '''
        flatDicts = []
        for dic in result_list:
            flatDicts.append(self.flatten_dict(dic.serialize()))
        print ("Procesados %d registros"% len(result_list))
        return pd.DataFrame.from_records(flatDicts)


    def append_df(self, parameters = {}, partition=False):
        '''
            Extracts all elements in multiple pages in a looping fashion,
            getting to the next page until a KeyError is raised.
            Then, appends each dataframe by page order.
            dfList is a list of pd.DataFrame objects.
            Returns the appended DataFrame.
        '''
        #No we use client temba

        dfList = []
        if "before" in  parameters or "after" in parameters:
            before = parameters['before'] if "before" in parameters else ""
            after = parameters['after'] if "after" in parameters else ""
            print ("after=%s&before=%s" %(after,before))
            result_list = self.get_client_request(before =before, after = after).all(retry_on_rate_exceed=True)
        else:
            result_list = self.get_client_request(parameters).all(retry_on_rate_exceed=True)

        df = self.to_df(result_list)
        # Append dataframes in a single one
        if len(df.index) == 0:
            return None

        else:
            return df

    def uuid_flow(self, flow):
        '''
            type(flow) = str
            Returns the UUID that corresponds with the name of the flow.
            Notice: this dataset is generated by the Get_flows module.
        '''

        # Import dataset into dataframe
        df = self.df_raw_flows
        # Narrow on observations with desired flow name
        rows = df.loc[df['name']==flow, 'uuid']


        # Policy: if there are many rows, return first value
        return rows.iloc[0]



class GetFlowDefinition():
    def __init__(self, client_io):
        self.flow_dict = {}
        self.client_io = client_io

    def search_flow(self, uuid):
        if uuid in self.flow_dict.keys():
            return self.flow_dict[uuid]
        else:
            #We have to ask for the definition of flow
            definition = self.client_io.get_definitions(flows=uuid, dependencies='none')
            if definition.flows:
                #Add all flows of metadata info#
                for flow in definition.flows:
                    self.flow_dict[flow['metadata']['uuid']] = flow
                #If flow uuid exist
                if uuid in self.flow_dict:
                    return self.flow_dict[uuid]
            else :
                self.flow_dict[uuid] = {}
            return {}


class GetRuns(Get):
    '''
        Inherited Class that deals with runs get requests.
    '''
    def __init__(self):
        super(GetRuns, self).__init__()
        self.flow_dict = {}

    ############ rapidpro client ############
    def get_client_request(self,before = None, after = None):
        return self.client_io.get_runs(before = before, after = after)

    def select_data(self, run, flow_manager):
        '''
            add field 'origin' to steps and values
        '''

        run_result = {}
        # Add all run-level information
        keys = list(run.keys())
        ## Remove steps and values entries
        for el in ['path', 'values']:
            keys.remove(el)
        for key in keys:
            run_result[key] = run[key]

        run_result['entries'] = []
        #Create dic of values base on node id
        value_nodes = {}
        mistake_nodes = {}
        for key in run['values']:
            value_entry = run['values'][key]
            value_nodes[value_entry['node']] = value_entry
            value_nodes[value_entry['node']]['label'] = key
            mistake_nodes[value_entry['node']] = 0

        flow_def = flow_manager.search_flow(run['flow']['uuid'])
        path_nodes = set([path['node'] for path in run['path']])

        #Check mistakes
        for idx in range(len(run['path'])-2):
            if run['path'][idx]['node'] in mistake_nodes:
                if run['path'][idx]['node'] == run['path'][idx+2]['node']:
                    mistake_nodes[run['path'][idx]['node']] += 1
        # Add field 'origin' to steps and values

        for node in path_nodes:
            entry = {}
            #Now, check if entry exist in values
            if node in value_nodes.keys():
                entry = value_nodes[node]
                entry['origin'] = 'values'
                entry['type'] = None
                entry['mistakes'] = mistake_nodes[node]
                run_result['entries'].append(entry)
            else:
                entry = sorted([path for path in run['path']if path["node"]== node],
                key =lambda x : x['time'])[0]
                entry['origin'] = 'steps'
                entry['category'] = None
                entry['label'] = 0
                entry['mistakes'] = 0
                entry['value'] = None
                if not flow_def:
                    entry['text'] = "unknown flow"
                    entry['type'] = "unknown flow"
                else:
                    action_def =[act['actions'] for act in flow_def['action_sets'] if act['uuid']== node]
                    if action_def: #We are not working with ruleset
                        action_def = action_def[0][0]
                        if "msg" in action_def:
                            if 'spa' in action_def['msg']:
                                #Spanish messages have different config
                                entry['text'] = action_def['msg']['spa']
                            else:
                                entry['text'] = action_def['msg']
                        entry['type'] = action_def['type']
                    run_result['entries'].append(entry)
        return run_result


class ProcessRuns(Get):
    '''
        Inherited class that adds key information to runs data
    '''
    def __init__(self):
        super(ProcessRuns, self).__init__()
        self.df_raw_flows = pd.read_csv(root + raw_flows)



    def tweaks(self, run):
        '''
            Executes multiple minor procedures:
                Remove ugly characters
                Sort steps
                Add chronological numbering to every step in 'steps_fdv'
                Add flow name to run-level data
        '''

        # Remove ugly chars
        for step in run['entries']:
            if type(step['value']) == str:
                step['value'] = step['value'].replace('\n', '').replace(u'\u23CE','')

        # Sort steps chronologically
        run['entries'] = sorted(run['entries'],
                                  key= lambda x: x['time'])

        # Add numbering
        i = 1
        for dic in run['entries']:
            dic['order'] = i
            i = i+1

        # Retrieve flow name
        return run



class ExportRuns(Get):
    '''
        Inherited class that exports runs get requests to .csv
    '''
    def __init__(self):
        super(ExportRuns, self).__init__()
        self.flow_manager = GetFlowDefinition(self.client_io)

    ############ rapidpro client ############
    def get_client_request(self,before = None, after = None):

        return self.client_io.get_runs(before = before, after=after)


    def add_common_key_entry(self, run, entry_dict, common_keys):
        primitive = (string_types, bool,int, float, complex)
        for key in common_keys:
            #Entries was added in last step
            if type(run[key]) is dict:
                for key2 in run[key].keys():
                    entry_dict[key+'_'+key2] = run[key][key2]
            elif isinstance(run[key], primitive):
                entry_dict[key] = run[key]
            else:
                pass
        return entry_dict

    def flatten_runs(self, runs):
        '''
            Returns a list whose entries represent steps.
            Also, it flattens the category, within 'steps_fdv'
        '''
        dic_entries = []
        common_keys = [u'exited_on', u'flow', u'responded', u'created_on', u'contact', u'modified_on', u'id', u'exit_type']

        for run in runs:
            for entry in run['entries']:
                dic_entries.append(self.add_common_key_entry(run, entry, common_keys))
            if not run['entries']:
                dic_entries.append(self.add_common_key_entry(run, {}, common_keys ))


        entries_df = pd.DataFrame(dic_entries)
        return entries_df


    def to_df(self, result_list):
        '''
            This function overrides the one in getMom.
            It is a wrapper: gets data, processes, flattens and returns
            a pandas df.
        '''
        # Get
        getter = GetRuns()
        processer = ProcessRuns()
        raw_runs = []
        runs = []
        for dic in result_list:
            raw_runs.append(dic.serialize())
        for raw_run in raw_runs:
            run = getter.select_data(raw_run,self.flow_manager)
            run = processer.tweaks(run)
            runs.append(run)
        # Export
        return self.flatten_runs(runs)

    def append_to_csv(self, df, header="False"):
        file_run = root + raw_runs + 'runs.csv'
        if not df is None:
            with open(file_run, 'a') as f:
                df.to_csv(f, header=header,index=False, encoding='utf-8')

    def export_runs(self, parameters = {}):
        '''
            (i)downloads all runs in paritions ,
            (ii)divide between values an runs and select relevant data,
            (iii)Sort by nodes by time
            (iv)saves DataFrame to a .csv
        '''
        if parameters:
            df = self.append_df(parameters=parameters, partition=True)
            df.to_csv(root + raw_runs + 'runs.csv', index=False, encoding='utf-8')
        else:
            #Divide flow by date
            #Check history to obtain last processed

            file_run = root + raw_runs + 'runs.csv'
            if (os.path.isfile(file_run)):
                tail_file = tailer.tail(open(file_run), 1)[0]
                df_tmp = pd.read_csv(StringIO(tail_file),header=None)
                base_date_str = df_tmp[11][0]
                base_date =dateutil.parser.parse(base_date_str).replace(tzinfo=None)

            else :
                #First partition, unknown size
                base_date =  datetime(2015, 5, 1,23, 58, 24, 268244)
                base_date_str = str(base_date.isoformat())
                parameters = {'before': base_date_str}
                df = self.append_df(parameters=parameters, partition=True)
                self.append_to_csv(df, header =True)
            delta = datetime.utcnow() - base_date
            delta = delta/PARTITION_NUMBER
            delta_time = base_date + delta
            delta_time_str = ""

            for counter in range(PARTITION_NUMBER):
                delta_time_str = str(delta_time.isoformat())
                base_date_str = str(base_date.isoformat())
                parameters = {'after': base_date_str, 'before': delta_time_str}
                df = self.append_df(parameters=parameters, partition=True)
                self.append_to_csv(df, header =False)
                base_date = delta_time
                delta_time += delta
                if counter % 10 == 0:
                    print ("---> Procesados %i de %i divisiones" %(counter+1, PARTITION_NUMBER))
                counter += 1
            parameters = {'after': delta_time_str}
            df = self.append_df(parameters=parameters, partition=True)
            self.append_to_csv(df, header =False)


    def append_runs(self, parameters = {}):
        '''
            Appends new runs data to runs information.
            Gets last modified date and runs request
        '''

        # Get date of last run
        df = pd.read_csv(root + raw_runs + 'runs.csv', dtype='unicode')
        df = df.sort_values('time', na_position='first')
        last_date = df['time'].iloc[-1]

        # Get observations after this date
        new_df = self.append_df({'after':last_date})
        new_df = new_df.sort_values('time')

        # Blow first run (rapidpro's 'after' is inclusive)
        try:
            index = 0
            while new_df['time'].iloc[index] == last_date:
                index += 1
            new_df = new_df.iloc[index:]
        except IndexError:
            print('No new information available')
            return None

        # Append to main df
        df = df.append(new_df, ignore_index=True)

        # Export
        df.to_csv(root + raw_runs + 'runs.csv', index=False, encoding='utf-8')

        # Check things went well
        #size = len(new_df.index)
        #print(df['modified_on'].tail(n=size+5))
        print('Runs Apendeados')


    def export_flow(self, flow, parameters = {}):
        '''
            type(flow) = str
            This function exports all runs of the specified flow.
                (i)downloads all runs pages,
                (ii)flattens and assembles the dictionaries for each runs page,
                (iii)sends each runs page to DataFrame,
                (iv)Appends all DataFrames,
                (v)saves resulting DataFrame to a .csv
        '''

        # Retrieve UUID
        uuid = self.uuid_flow(flow)

        # Set request details
        params = {'flow_uuid':uuid}
        parameters.update(params)

        # Assemble dataframe
        appendedDf = self.append_df(parameters)

        # Export as .csv
        appendedDf.to_csv(root + raw_runs + flow + '.csv',
                          index = False,
                          encoding = 'utf-8')




class GetContacts(Get):
    '''
        Inherited Class that deals with contacts get requests.
    '''

    ############ rapidpro client ############
    def get_client_request(self,before = None, after = None):
        return self.client_io.get_contacts(before = before, after = after)



    def export_contacts(self, parameters={}, path=root + raw_contacts):
        '''
            (i)downloads the contacts,
            (ii)flattens and assembles the dictionaries,
            (iii)sends data to DataFrame
            (iv)removes a useless contact field (with varname so long that STATA
                cannot handle
            (v)saves DataFrame to a .csv
            path is the full path to new .csv, string
        '''

        df = self.append_df(parameters)
        df.to_csv(path, encoding='utf-8', index = False)




class GetFields(Get):
    '''
        Inherited Class that deals with contact fields get requests.
    '''

    ############ rapidpro client ############
    def get_client_request(self, parameters = {}):
        return self.client_io.get_fields(parameters)


    def export_fields(self, parameters={}):
        '''
            (i)downloads the fields,
            (ii)flattens and assembles the dictionaries,
            (iii)sends data to DataFrame
            (iv)saves DataFrame to a .csv
        '''

        df = self.append_df(parameters)
        df.to_csv(root + raw_fields, encoding='utf-8', index = False)




class GetFlows(Get):
    '''
        Inherited Class that deals with flows get requests.
    '''

    ############ rapidpro client ############
    def get_client_request(self, parameters = {}):
        return self.client_io.get_flows(parameters)

    def export_flows(self, parameters = {}):
        '''
            (i)downloads the flows,
            (ii)flattens and assembles the dictionaries,
            (iii)sends data to DataFrame
            (iv)saves DataFrame to a .csv
        '''

        df = self.append_df(parameters)
        df.to_csv(root + raw_flows, index=False, encoding = 'utf-8')




class GetGroups(Get):
    '''
        Inherited Class that deals with groups get requests.
    '''

    ############ rapidpro client ############
    def get_client_request(self, parameters = {}):
        return self.client_io.get_groups(parameters)

    def export_groups(self, parameters={}):
        '''
            (i)downloads the groups,
            (ii)flattens and assembles the dictionaries,
            (iii)sends data to DataFrame
            (iv)saves DataFrame to a .csv
        '''

        df = self.append_df(parameters)
        df.to_csv(root + raw_groups, encoding='utf-8', index = False)




class GetMessages(Get):
    '''
        Inherited Class that deals with messages get requests.
    '''

    ############ rapidpro client ############
    def get_client_request(self, parameters = {}):
        return self.client_io.get_messages(**parameters)


    def to_df(self, result_list):
        '''
            Runs a request, extracts messages and assembles them.
        '''


        # raw should be a list of dicts. Flatten them and append to new list
        flatDicts = []
        for item in result_list:
            dic = item.serialize()
            for char in ['"', "'", ";", ",", '\u2013', '\u2026', '\r\n']:
                dic['text'] = dic['text'].replace(char, '')
            flatDicts.append(self.flatten_dict(dic))

        return pd.DataFrame.from_records(flatDicts)


    def export_messages(self, parameters={}):
        '''
            (i)downloads the messages,
            (ii)flattens and assembles the dictionaries,
            (iii)sends data to DataFrame
            (iv)saves DataFrame to a .csv
        '''

        df = self.append_df(parameters)
        df.to_csv(root + raw_messages, encoding='utf-8', index = False)
