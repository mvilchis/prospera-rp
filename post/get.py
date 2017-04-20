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
from temba_client.v2 import TembaClient
import sys

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
PARTITION_NUMBER = 100

class Get(object):

    '''
        Encompasses all functions related to getting RapidPro messages and
        incorporating them to our master dataset.
    '''
    def __init__(self):
        super(Get, self).__init__()
        self.df_raw_flows = pd.read_csv(root + raw_flows)
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
        print ("Realizando request")
        if partition and not parameters:
            base_date =  datetime(2015, 1, 1,23, 58, 24, 268244)
            base_date_str = str(base_date.isoformat())
            result = self.get_client_request(before = base_date_str).all(retry_on_rate_exceed=True)
            delta = datetime.utcnow() - base_date
            delta = delta/PARTITION_NUMBER
            part_time = base_date + delta
            part_time_str = ""
            counter = 1
            for i in range(PARTITION_NUMBER):
                part_time_str = str(part_time.isoformat())
                base_date_str = str(base_date.isoformat())
                result += self.get_client_request(before = part_time_str,
                                                  after = base_date_str).all(retry_on_rate_exceed=True)
                base_date = part_time
                part_time += delta
                if counter % 10 == 0:
                    print ("---> Procesados %i de %i divisiones" %(counter, PARTITION_NUMBER))
                counter += 1
            result += self.get_client_request(after=part_date_str).all(retry_on_rate_exceed=True)
            result_list = result
        else:
            result_list = self.get_client_request(parameters).all(retry_on_rate_exceed=True)
        print ("Fin de request")
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


    def name_flow(self, uuid, df):
        '''
            type(uuid) = str
            Returns the name that corresponds with the uuid of the flow.
            Notice: this dataset is generated by the Get_flows module.
        '''

        # Narrow on row
        rows = df.loc[df['uuid']==uuid, 'name']


        # Policy: if there are many rows, return first value
        if len(rows) ==0: ##Not in the dataframe then search
            r = requests.get('https://api.rapidpro.io/api/v2/flows.json',
                                            headers = {'Authorization': rp_api},
                                            params = {'uuid': uuid})
            result =  r.json()['results']
            if not result:
                return 'Missing'
            return result[0]['name']
        return rows.iloc[0]




class GetRuns(Get):
    '''
        Inherited Class that deals with runs get requests.
    '''

    ############ rapidpro client ############
    def get_client_request(self,before = None, after = None):
        return self.client_io.get_runs(before = before, after = after)

    def select_data(self, run):
        '''
            add field 'origin' to steps and values
        '''

        run_result = {}

        # Add all run-level information
        keys = list(run.keys())
        ## Remove steps and values entries
        for el in ['steps', 'values']:
            keys.remove(el)
        for key in keys:
            run_result[key] = run[key]

        # Add field 'origin' to steps and values
        for entry in run['steps']:
            entry['origin'] = 'steps'
        for entry in run['values']:
            entry['origin'] = 'values'

        run_result['entries'] = []

        # Add all values entries with arrived_on and left_on info from steps
        for step_values in run['values']:
            for step_steps in run['steps']:
                if step_steps['left_on'] == step_values['time']:
                    for field in ['arrived_on', 'left_on']:
                        step_values[field] = step_steps[field]
                    run_result['entries'].append(step_values)
                    # Remove steps entry
                    run['steps'].remove(step_steps)
                    break

        # Add all remaining steps
        for step_steps in run['steps']:
            for field in [ 'category',
                           'label',
                           'rule_value',
                           'time' ]:
                step_steps[field] = None
            run_result['entries'].append(step_steps)

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
        for i in range(len(run['entries'])):
            step = run['entries'][i]
            for field in ['rule_value',
                          'text',
                          'value']:
                if type(step[field]) == str:
                    for el in ['\n', u'\u23CE']:
                        step[field] = step[field].replace('el', '')

        # Sort steps chronologically
        run['entries'] = sorted(run['entries'],
                                  key=itemgetter('arrived_on'))

        # Add numbering
        i = 1
        for dic in run['entries']:
            dic['order'] = i
            i = i+1

        # Retrieve flow name
        run['flow_name'] = self.name_flow(run['flow_uuid'], self.df_raw_flows)

        return run



    def get_repetitions(self, steps, index):
        '''
            Computes the number of repeated nodes that appear side by side in a
            chronologically sorted list of steps.
            index is an int, the starting point to start counting.
        '''

        result = 0
        tracker = index

        try:
            while steps[tracker]['node'] == steps[tracker+1]['node']:
                result += 1
                tracker += 1

        except IndexError:
            return len(steps[index: ]) - 1

        return result


    def get_repetitions_test(self):
        steps1 = [{'node': 12}, {'node': 12}, {'node': 3213}, {'node': 3123123},
                  {'node': 414}]
        steps2 = [{'node': 1234}, {'node': 534}, {'node': 534}, {'node': 534}, {'node':
                                                                               1}]
        steps3 = [{'node': 1}, {'node': 1}, {'node': 1}, {'node': 1}, {'node': 1}]
        steps4 = [{'node': 52}, {'node': 42}, {'node': 123}, {'node': 123}, {'node': 123}]
        for steps in [steps1, steps2, steps3, steps4]:
            print("----------------------------------------------------------------------------------------------------")
            print(self.get_repetitions(steps, 0))
            print("------------------","------------------","------------------")
            print(self.get_repetitions(steps, 2))
            print("------------------","------------------","------------------")
            print(self.get_repetitions(steps, -1))


    def add_mistakes(self, run):
        '''
            Adds number of mistakes (i.e. repetition of contiguous nodes in a
            chronologically ordered sequence of steps minus 1).
        '''

        # Start by adding mistakes key to all run
        for step in run['entries']:
            step['mistakes'] = 0

        # Now fill in
        current = 0

        while True:
            mistakes = self.get_repetitions(run['entries'], current)

            for index in [ x+current for x in range(mistakes+1) ]:
                run['entries'][index]['mistakes'] = mistakes

            current = current + mistakes + 1

            if current > len(run['entries']) - 1:
                break

        return run


    def get_etime(self, run, index=-1):
        try:
            etime = run['entries'][index]['left_on'][:19]
            return etime
        except TypeError:
            return self.get_etime(run, index-1)


    def run_duration(self, run):
        '''
            Adds the total seconds elapsed between the start and end of a
            contact's interaction during a run.
        '''

        if  len(run['entries']) < 2:
            run['run_time'] = None

        else:
            run['entries'] = sorted(run['entries'],
                                          key=itemgetter('arrived_on'))

            # Start
            stime = run['entries'][0]['arrived_on'][:19]
            stime = datetime.strptime(stime, '%Y-%m-%dT%H:%M:%S')
            # End
            etime = self.get_etime(run)
            etime = datetime.strptime(etime, '%Y-%m-%dT%H:%M:%S')

            finish_time = etime-stime
            run['run_time'] = finish_time.total_seconds()

        return run


    def step_duration(self, run):
        '''
            Adds the total seconds elapsed between the start and end of
            a step where contact interaction is required.
        '''

        for step in run['entries']:

            if step['left_on'] is None:
                step['step_time'] = None

            else:
                stime = step['arrived_on'][:19]
                start = datetime.strptime(stime, '%Y-%m-%dT%H:%M:%S')
                etime = step['left_on'][:19]
                end = datetime.strptime(etime, '%Y-%m-%dT%H:%M:%S')

                finish_time = end - start
                step['step_time'] = finish_time.total_seconds()

        return run


    def response_type(self, run):
        '''
            extracts the response type from 'label'
            Response types:
                - Yes/No -------- s
                - Categorical --- c
                - Datetime ------ f
                - Numerical ----- n
                - Text ---------- t
        '''

        for step in run['entries']:

            step['response_type'] = None

            flag = 1
            suffixes = ['_s', '_c', '_f', '_n', '_t']

            if step['label'] == None:
                pass
            else:
                for suf in suffixes:
                    if step['label'][-2:] == suf:
                        rType = suf[-1]
                        step['response_type'] = rType
                        break
                    else:
                        pass

        return run





class ExportRuns(Get):
    '''
        Inherited class that exports runs get requests to .csv
    '''
    ############ rapidpro client ############
    def get_client_request(self,before = None, after = None):

        return self.client_io.get_runs(before = before, after=after)


    def flatten_run(self, run):
        '''
            Returns a list whose entries represent steps.
            Also, it flattens the category, within 'steps_fdv'
        '''

        result = []

        # Flatten 'category' if it is assigned a dict
        for step in run['entries']:
            if type(step['category']) == dict:
                for key in step['category'].keys():
                    step['category'+'_'+key] = step['category'][key]
                del step['category']
            else:
                step['category_base'] = step.pop('category')
                step['category_spa'] = None

        # Create list of dictionaries at the step level
        run_level = copy.deepcopy(run)
        del run_level['entries']

        if len(run['entries']) > 0:
            for step in run['entries']:
                step.update(run_level)
                result.append(step)

        # No 'entries' imply no type 'R' step --> append_step function
        else:
            pass

        return result


    def to_df(self, result_list):
        '''
            This function overrides the one in getMom.
            It is a wrapper: gets data, processes, flattens and returns
            a pandas df.
        '''

        # Get
        getter = GetRuns()
        runs = []
        processer = ProcessRuns()

        for dic in result_list:
            run = getter.select_data(dic.serialize())
            run = processer.tweaks(run)
            run = processer.add_mistakes(run)
            run = processer.run_duration(run)
            run = processer.step_duration(run)
            run = processer.response_type(run)
            runs.append(run)

        # Export
        flat = []
        for run in runs:
            flat.extend(self.flatten_run(run))

        ## Into dataframe
        return pd.DataFrame.from_records(flat)


    def export_runs(self, parameters = {}):
        '''
            (i)downloads the contacts,
            (ii)flattens and assembles the dictionaries,
            (iii)sends data to DataFrame
            (iv)removes a useless contact field (with varname so long that STATA
                cannot handle
            (v)saves DataFrame to a .csv
        '''
        df = self.append_df(parameters=parameters, partition=True)
        df.to_csv(root + raw_runs + 'runs.csv', index=False, encoding='utf-8')


    def append_runs(self, parameters = {}):
        '''
            Appends new runs data to runs information.
            Gets last modified date and runs request
        '''

        # Get date of last run
        df = pd.read_csv(root + raw_runs + 'runs.csv', dtype='unicode')
        df = df.sort_values('modified_on', na_position='first')
        last_date = df['modified_on'].iloc[-1]

        # Get observations after this date
        new_df = self.append_df({'after':last_date})
        new_df = new_df.sort_values('modified_on')

        # Blow first run (rapidpro's 'after' is inclusive)
        try:
            index = 0
            while new_df['modified_on'].iloc[index] == last_date:
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
    def get_client_request(self,before = None, after = None):
        return self.client_io.get_messages(before = before, after = after)


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
