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


#configuration
config = configparser.ConfigParser()
config.read('keys.ini')
## Paths
root = config['paths']['root']
raw_flows = config['paths']['raw_flows']
raw_runs = config['paths']['raw_runs']
raw_contacts = config['paths']['raw_contacts']
raw_messages = config['paths']['raw_messages']
raw_fields = config['paths']['raw_fields']
raw_groups = config['paths']['raw_groups']
## Rapidpro
rp_api = config['rapidpro']['rp_api']




class Get(object):
    '''
        Encompasses all functions related to getting RapidPro messages and
        incorporating them to our master dataset.
    '''

    def request_get(self, header = {}, parameters = {}):
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


    def to_df(self, parameters = {}):
        '''
            Runs a request, extracts messages and assembles them.
        '''

        raw = self.request_get(parameters = parameters)

        # raw should be a list of dicts. Flatten them and append to new list
        flatDicts = []
        for dic in raw:
            flatDicts.append(self.flatten_dict(dic))

        return pd.DataFrame.from_records(flatDicts)


    def append_df(self, parameters = {}):
        '''
            Extracts all elements in multiple pages in a looping fashion,
            getting to the next page until a KeyError is raised.
            Then, appends each dataframe by page order.
            dfList is a list of pd.DataFrame objects.
            Returns the appended DataFrame.
        '''

        dfList = []
        p = 1

        # Get list of dataframes, one dataframe per page
        while True:
            params = parameters.copy()
            params.update({'page':p})

            try:
                df = self.to_df(params)
                dfList.append(df)
                p += 1
            except KeyError:
                break

        # Append dataframes in a single one
        if len(dfList) == 0:
            return None

        else:
            outDf = dfList[0]

            if len(dfList) > 1:
                outDf = outDf.append(dfList[1:], ignore_index=True)

            return outDf

    def uuid_flow(self, flow):
        '''
            type(flow) = str
            Returns the UUID that corresponds with the name of the flow.
            Notice: this dataset is generated by the Get_flows module.
        '''

        # Import dataset into dataframe
        df = pd.read_csv(root + raw_flows)

        # Narrow on observations with desired flow name
        rows = df.loc[df['name']==flow, 'uuid']

        # Policy: if there are many rows, return first value
        return rows.iloc[0]


    def name_flow(self, uuid):
        '''
            type(uuid) = str
            Returns the name that corresponds with the uuid of the flow.
            Notice: this dataset is generated by the Get_flows module.
        '''
        # Import dataset into dataframe
        df = pd.read_csv(root + raw_flows)

        # Narrow on row
        rows = df.loc[df['uuid']==uuid, 'name']
        print(rows)

        # Policy: if there are many rows, return first value
        return rows.iloc[0]




class GetRuns(Get):
    '''
        Inherited Class that deals with runs get requests.
    '''


    def request_get(self, parameters = {}):
        '''
            runs get request on RapidPro's API with the appropriate authentication commands.
            param is a dict of query parameters to be specified.
        '''

        r = requests.get('https://api.rapidpro.io/api/v1/runs.json',
                                        headers = {'Authorization': rp_api},
                                        params = parameters)
        return r.json()['results']


    def select_data(self, run, step_fields = ['arrived_on', 'left_on']):
        '''
            Run-level function (a run is an entry in self.request())
            Returns a dict with selected information from run.
            This dict contains all run-level information, i.e.
                completed
                contact
                created_on
                expired_on
                expires_on
                flow
                flow_uuid
                modified_on
                run
            and every entry in 'values', which is a list of node-level info.
            All entries in 'values' are also added with step_fields info from
            entries in steps.
            step_fields is a list of strings (data fields)
            Additionally, if the run was not completed, the dict contains
            the last step recorded with 'type' 'R'.
        '''

        run_result = {}

        # Add all run-level information
        keys = list(run.keys())
        ## Remove steps and values entries
        for el in ['steps', 'values']:
            keys.remove(el)
        for key in keys:
            run_result[key] = run[key]

        # Add every element in 'values' to new field: 'steps_selected'
        run_result['steps_selected'] = copy.deepcopy(run['values'])

        for step_values in run_result['steps_selected']:
            for step_steps in run['steps']:
                if step_values['time'] == step_steps['left_on']:
                    for field in step_fields:
                        step_values[field] = step_steps[field]
                else:
                    pass

        # Append last of steps to steps_selected if type 'R' AND not already in steps_selected.
        ## Get sorted list of times when contact left step in values
        values_times = [ entry['time'] for entry in run['values'] ]
        values_times = sorted(values_times)

        ## Trick to catch flows with no contact reaction
        if values_times == []:
            values_times = ['']

        ## Sort 'steps' by 'arrived_on'
        run['steps'] = sorted(run['steps'], key=itemgetter('arrived_on'))

        ## Get last entry with type R. If there is no such entry, exit
        index = -1
        try:
            while run['steps'][index]['type'] != 'R':
                index -= 1
        except IndexError:
            return run_result

        last = run['steps'][index]

        ## If last entry in steps occurred after last entry in values, append
        if (last['arrived_on'] > values_times[-1]):
            run_result['steps_selected'].append(
                {'category': None,
                 'label': None,
                 'rule_value': None,
                 'text': None,
                 'value': None})
            run_result['steps_selected'][-1]['node'] = last['node']
            run_result['steps_selected'][-1]['time'] = last['arrived_on']
            run_result['steps_selected'][-1]['arrived_on'] = last['arrived_on']
            run_result['steps_selected'][-1]['left_on'] = last['left_on']

        return run_result


    def select_data_test():
        pp = pprint.PrettyPrinter()
        inst = GetRuns()
        json = inst.request_get()[:20]
        pp.pprint(json)
        print("--------------------------------------------------------------------------------")
        for run in json:
            pp.pprint(inst.select_data(run))




class ProcessRuns(Get):
    '''
        Inherited class that adds key information to runs data
    '''


    def tweaks(self, run):
        '''
            Executes multiple minor procedures:
                Remove ugly characters
                Sort steps
                Add chronological numbering to every step in 'steps_fdv'
                Add flow name to run-level data
        '''

        # Remove ugly chars
        for i in range(len(run['steps_selected'])):
            step = run['steps_selected'][i]
            for field in ['rule_value',
                          'text',
                          'value']:
                if type(step[field]) == str:
                    step[field] = step[field].replace('\n', '')

        # Sort steps chronologically
        run['steps_selected'] = sorted(run['steps_selected'],
                                  key=itemgetter('arrived_on'))

        # Add numbering
        i = 1
        for dic in run['steps_selected']:
            dic['order'] = i
            i = i+1

        # TODO: check this function works
        # Retrieve flow name
        #run['flow_name'] = self.name_flow(run['flow_uuid'])

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
        for step in run['steps_selected']:
            step['mistakes'] = 0

        # Now fill in
        current = 0

        while True:
            mistakes = self.get_repetitions(run['steps_selected'], current)

            for index in [ x+current for x in range(mistakes+1) ]:
                run['steps_selected'][index]['mistakes'] = mistakes

            current = current + mistakes + 1

            if current > len(run['steps_selected']) - 1:
                break

        return run


    def add_mistakes_test(self):
        getter = GetRuns()
        pp = pprint.PrettyPrinter()
        json = getter.request_get()[:20]
        pp.pprint(json)
        print("--------------------------------------------------------------------------------")
        for run in json:
            pp.pprint(self.add_mistakes(self.tweaks(getter.select_data(run))))


    def run_duration(self, run):
        '''
            Adds the total seconds elapsed between the start and end of a
            contact's interaction during a run.
        '''

        if  len(run['steps_selected']) < 2:
            run['run_time'] = None

        else:
            run['steps_selected'] = sorted(run['steps_selected'],
                                          key=itemgetter('arrived_on'))

            # Start
            stime = run['steps_selected'][0]['arrived_on'][:19]
            stime = datetime.strptime(stime, '%Y-%m-%dT%H:%M:%S')
            # End
            try:
                etime = run['steps_selected'][-1]['left_on'][:19]
            except TypeError:
                etime = run['steps_selected'][-2]['left_on'][:19]
            etime = datetime.strptime(etime, '%Y-%m-%dT%H:%M:%S')

            finish_time = etime-stime
            run['run_time'] = finish_time.total_seconds()

        return run


    def run_duration_test(self):
        getter = GetRuns()
        pp = pprint.PrettyPrinter()
        json = getter.request_get()[:20]
        pp.pprint(json)
        print("--------------------------------------------------------------------------------")
        for run in json:
            pp.pprint(self.run_duration(self.add_mistakes(self.tweaks(getter.select_data(run)))))


    def step_duration(self, run):
        '''
            Adds the total seconds elapsed between the start and end of
            a step where contact interaction is required.
        '''

        for step in run['steps_selected']:

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


    def step_duration_test(self):
        getter = GetRuns()
        pp = pprint.PrettyPrinter()
        json = getter.request_get()[:20]
        pp.pprint(json)
        print("--------------------------------------------------------------------------------")
        for run in json:
            pp.pprint(self.step_duration(self.run_duration(self.add_mistakes(self.tweaks(getter.select_data(run))))))


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

        for step in run['steps_selected']:

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


    def step_duration_test(self):
        getter = GetRuns()
        pp = pprint.PrettyPrinter()
        json = getter.request_get()[:20]
        pp.pprint(json)
        print("--------------------------------------------------------------------------------")
        for run in json:
            pp.pprint(self.response_type(self.step_duration(self.run_duration(self.add_mistakes(self.tweaks(getter.select_data(run)))))))




class ExportRuns(Get):
    '''
        Inherited class that exports runs get requests to .csv
    '''

    def flatten_run(self, run):
        '''
            Returns a list whose entries represent steps.
            Also, it flattens the category, within 'steps_fdv'
        '''

        result = []

        # Flatten 'category' if it is assigned a dict
        for step in run['steps_selected']:
            if type(step['category']) == dict:
                for key in step['category'].keys():
                    step['category'+'_'+key] = step['category'][key]
                del step['category']
            else:
                step['category_base'] = step.pop('category')
                step['category_spa'] = None

        # Create list of dictionaries at the step level
        run_level = copy.deepcopy(run)
        del run_level['steps_selected']

        if len(run['steps_selected']) > 0:
            for step in run['steps_selected']:
                step.update(run_level)
                result.append(step)


        # No 'steps_fdv' imply no type 'R' step --> append_step function
        else:
            pass

        return result


    def flatten_run_test(self):
        getter = GetRuns()
        processer = ProcessRuns()
        pp = pprint.PrettyPrinter()
        json = getter.request_get()[:20]
        pp.pprint(json)
        print("--------------------------------------------------------------------------------")
        for run in json:
            pp.pprint(self.flatten_run(processer.response_type(processer.step_duration(processer.run_duration(processer.add_mistakes(processer.tweaks(getter.select_data(run))))))))


    def to_df(self, parameters = {}):
        '''
            This function overrides the one in getMom.
            It is a wrapper: gets data, processes, flattens and returns
            a pandas df.
        '''

        # Get
        getter = GetRuns()
        runs_raw = getter.request_get(parameters = parameters)
        runs = [None]*len(runs_raw)
        for i in range(len(runs_raw)):
            runs[i] = getter.select_data(runs_raw[i])
        del runs_raw

        # Process
        processer = ProcessRuns()
        for run in runs:
            run = processer.tweaks(run)
            run = processer.add_mistakes(run)
            run = processer.run_duration(run)
            run = processer.step_duration(run)
            run = processer.response_type(run)

        # Export
        flat = []
        for run in runs:
            flat.extend(self.flatten_run(run))
        ## Into dataframe
        df = pd.DataFrame.from_records(flat)

        return df


    def to_df_test(self):
        getter = GetRuns()
        processer = ProcessRuns()
        pp = pprint.PrettyPrinter()
        json = getter.request_get()[:20]
        pp.pprint(json)
        print("--------------------------------------------------------------------------------")
        df = self.to_df()
        for col in df:
            print(df[col])


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
        appendedDf = self.append_df(parameters=parameters)
        
        print(appendedDf.loc[appendedDf['arrived_on'].isnull(), :])

        # Export as .csv
        appendedDf.to_csv(root + raw_runs + flow + '.csv',
                          index = False,
                          encoding = 'utf-8')




def export_runs():
    # TODO: Update this list
    inst = ExportRuns()

    l = ['auxAlta',
         'concerns1',
         'concerns10',
         'concerns11',
         'concerns12',
         'concerns13',
         'concerns14',
         'concerns15',
         'concerns16',
         'concerns17',
         'concerns18',
         'concerns2',
         'concerns3',
         'concerns4',
         'concerns5',
         'concerns6',
         'concerns7',
         'concerns8',
         'concerns9',
         'followUp_fdv',
         'getData_apptDate',
         'getData_dueDate',
         'getData_relayer',
         'incentivesCollect1',
         'incentivesCollect2',
         'incentivesCollect3',
         'incentivesCollect4',
         'incentivesCollect5',
         'incentivesCollectF1',
         'incentivesCollectF2',
         'incentivesInform',
         'incentives_date1',
         'incentives_date2',
         'miAlerta',
         'miAlerta_followUp',
         'miAlta_apptDate',
         'miAlta_dueDate',
         'miAlta_init',
         'miAlta_init',
         'miAlta_selfAppt_old',
         'miAlta_selfWshp',
         'miAlta_update',
         'miAlta_whPr',
         'miAlta_whPr',
         'miCambio',
         'miPrueba2',
         'miPrueba_cat',
         'miPrueba_fechas',
         'miPrueba_followUp',
         'miPrueba_num',
         'miPrueba_siNo',
         'miPrueba_text',
         'pregnant_puerperium',
         'prevent1',
         'prevent10',
         'prevent11',
         'prevent12',
         'prevent13',
         'prevent14',
         'prevent15',
         'prevent2',
         'prevent3',
         'prevent4',
         'prevent5',
         'prevent6',
         'prevent7',
         'prevent8',
         'prevent9',
         'reminders1.1',
         'reminders1.2',
         'reminders1.3',
         'reminders2.1',
         'reminders2.2',
         'reminders2.3',
         'reminders3.1',
         'reminders3.2',
         'reminders3.3',
         'reminders4.1',
         'reminders4.2',
         'reminders4.3',
         'reminders5.1',
         'reminders5.2',
         'reminders5.3',
         'remindersExtra1',
         'remindersExtra2',
         'remindersExtra3',
         'remindersFinal1',
         'remindersFinal2',
         'remindersFinal3',
         'prePiloto_planning1',
         'prePiloto_planning2',
         'prePiloto_planning3',
         'prePiloto_planning4',
         'prePiloto_planning5',
         'prePiloto_planning6',
         'prePiloto_planning7' ]

    for flow in l:
        inst.export_flow(flow)




class GetContacts(Get):
    '''
        Inherited Class that deals with contacts get requests.
    '''


    def request_get(self, parameters = {}):
        '''
            runs get request on RapidPro's API with the appropriate authentication commands.
            param is a dict of query parameters to be specified.
        '''

        r = requests.get('https://api.rapidpro.io/api/v1/contacts.json',
                                                    headers = {'Authorization': rp_api},
                                                    params = parameters)
        return r.json()['results']


    def export_contacts(self, parameters={}):
        '''
            (i)downloads the contacts,
            (ii)flattens and assembles the dictionaries,
            (iii)sends data to DataFrame
            (iv)removes a useless contact field (with varname so long that STATA
                cannot handle
            (v)saves DataFrame to a .csv
        '''

        df = self.append_df(parameters)
        df.to_csv(root + raw_contacts, encoding='utf-8', index = False)




class GetFields(Get):
    '''
        Inherited Class that deals with contact fields get requests.
    '''


    def request_get(self, parameters = {}):
        '''
            runs get request on RapidPro's API with the appropriate authentication commands.
            param is a dict of query parameters to be specified.
        '''

        r = requests.get('https://api.rapidpro.io/api/v1/fields.json',
                                                    headers = {'Authorization': rp_api},
                                                    params = parameters)
        return r.json()['results']


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


    def request_get(self, parameters = {}):
        '''
            runs get request on RapidPro's API with the appropriate authentication commands.
            param is a dict of query parameters to be specified.
        '''

        r = requests.get('https://api.rapidpro.io/api/v1/flows.json',
                                                    headers = {'Authorization': rp_api},
                                                    params = parameters)
        return r.json()['results']


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


    def request_get(self, parameters = {}):
        '''
            runs get request on RapidPro's API with the appropriate authentication commands.
            param is a dict of query parameters to be specified.
        '''

        r = requests.get('https://api.rapidpro.io/api/v1/groups.json',
                                                    headers = {'Authorization': rp_api},
                                                    params = parameters)
        return r.json()['results']


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


    def request_get(self, parameters = {}):
        '''
            runs get request on RapidPro's API with the appropriate authentication commands.
            param is a dict of query parameters to be specified.
        '''

        r = requests.get('https://api.rapidpro.io/api/v1/messages.json',
                                                    headers = {'Authorization': rp_api},
                                                    params = parameters)
        return r.json()['results']


    def to_df(self, parameters = {}):
        '''
            Runs a request, extracts messages and assembles them.
        '''

        raw = self.request_get(parameters = parameters)

        # raw should be a list of dicts. Flatten them and append to new list
        flatDicts = []
        for dic in raw:
            for char in ['"', "'", ";", ",", '\u2013', '\u2026', '\r\n']:
                        dic['text'] = dic['text'].replace(char, '')
            flatDicts.append(self.flatten_dict(dic))

        pp = pprint.PrettyPrinter()
        pp.pprint(raw)

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
