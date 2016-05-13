# -*- coding: utf-8 -*-

# This is the main file.
# It contains functions that miscellaneous tasks centered around the RapidPro
# API with a focus on interaction with external datasets.
# There is also a function to deal with input of Google spreadsheets

import json
import requests
import csv
import datetime
import configparser
import gspread
from oauth2client.client import SignedJwtAssertionCredentials
import numpy as np
import pandas as pd


# configuration
config = configparser.ConfigParser()
## file keys.ini should be in repository root
config.read(os.path.dirname(os.path.dirname(__file__)) + '/keys.ini')
## Google credentials
gCredentials = config['google']['credentials']
## Paths
root = config['paths']['root']
raw_contacts = config['paths']['raw_contacts']
raw_flows = config['paths']['raw_flows']
## RapidPro
rp_api = config['rapidpro']['rp_api']




def io(dbPath, subset=None):
    '''
        Reads a .csv into dataframe, all string, np.nan set to ''.
        subset is a list of varnames to import.
        The encoding is set to latin-1 since I assume the dataset comes from STATA 13 (or <13)
            handling. These STATA versions use this encoding. Unicode is supported only from
            STATA 14 onwards...
        dbPath is the full path to the dataset (starting at root, see beginning of file.)
        It's important to get everything as string: some integer cols are otherwise assigned
            a float type and when converted to string are displayed as floats...
    '''

    df = pd.read_csv(dbPath,
                     encoding= 'latin-1',
                     dtype = 'str',
                     usecols = subset)

    df.fillna('', inplace = True)

    # Stringify and strip left and right whitespace
    for col in df:
        df[col] = df[col].astype(str)
        df[col] = df[col].str.strip()

    # Print dataset results for the user to make sure that everything goes smoothly.
    print(df.dtypes)
    print(df.head(10))
    for col in df:
        print(col, type(df[col].iloc[0]))

    return df


def load_gspread(url):
    '''
        returns the first instance of class gspread.Worksheet() in spreadsheet located in url.
    '''

    # Construct credentials. You should have a .json file with credentials for GSheet get requests.
    json_key = json.load(open(root + gCredentials))
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = SignedJwtAssertionCredentials(json_key['client_email'],
                                                json_key['private_key'].encode(),
                                                scope)
    # Sign in
    gc = gspread.authorize(credentials)

    # Open spreadsheet. The url leads to the dataset
    book = gc.open_by_url(url)

    return book.sheet1


def read_gspread(url):
    '''
        returns a pandas dataframe of the google spreadsheet specified in url.
        The spreadsheet has to be shared with the corresponding Google service account
    '''
    
    # Load gspread
    sheet = load_gspread(url)

    # Convert sheet contents to a list of dicts, then convert to pandas dataframe
    df = pd.DataFrame(sheet.get_all_records())

    # Fill in missing values with empty strings
    df.fillna('', inplace = True)

    # Stringify
    for col in df:
        df[col] = df[col].astype(str)
    
    # Print dataset results for the user to make sure that everything goes smoothly.
    print(df.dtypes)
    print(df.head(10))
    for col in df:
        print(col, type(df[col].iloc[0]))

    return df


def rowAppend_gspread(url, values):
    '''
        values is a list of values or a list of lists of values.
        Inserts values in the first row of worksheet not yet populated with data.
        Assumes that row is not populated if first column is not populated.
    '''
    
    # Load gspread
    sheet = load_gspread(url)
    
    # Get index of first non-populated recursively
    def sheet_len(ws, index=1):
        if ws.cell(index, 1).value == '':
            return 1
        else:
            return sheet_len(ws, index+1) + 1
    
    # Insert rows specified in values
    last = sheet_len(sheet)
    if type(values[0]) == list:
        for vals in values:
            sheet.insert_row(vals, index=last)
            last += 1
    else:
        sheet.insert_row(values, index=last)

    return None


def update_fields(df, variables, date=None):
    '''
        Runs post requests to update contact fields associated in variables.
        Missing data are ignored and requests are executed with all available information.
        variables is a list or a dict that contains the correspondence of variable names
        in the current dataset (df) and RapidPro contact fields. e.g.
            {
                'var1': 'contact_field1',
                'var2': 'contact_field2'
            }
        If it is a list, then varnames and contact fields must match.
        date is today's date (to keep track of when things happened in RP) in format DD/MM/YYYYY
    '''

    for row in range(len(df.index)):
        # Assemble contact fields to update
        to_update = {}

        if type(variables) == dict:
            for field in variables.keys():
                if df[field].iloc[row] != '' :
                    to_update[variables[field]] = df[field].iloc[row]
                else:
                    pass

        elif type(variables) == list:
            for field in variables:
                if df[field].iloc[row] != '' :
                    to_update[field] = df[field].iloc[row]
                else:
                    pass

        if to_update != {}:
            if date != None:
                to_update['rp_datemodified'] = date
            else:
                pass
            print('--')
            print(to_update)
            print(df['phone'].iloc[row])
            print('--')

        # Proceed with request
        requests.post(
            'https://api.rapidpro.io/api/v1/contacts.json',
            headers = { 'content-type': 'application/json',
                        'Authorization': rp_api },
            data = json.dumps( { 'urns': [df['phone'].iloc[row]],
                                 'fields': to_update } )
        )

    return None


def get_uuids(df):
    '''
        retrieves the contacts' uuids. The merge is on phone number
        TODO: should it return list of uuids?
    '''

    # Call contacts with io function
    contact_uuids = io(root + raw_contacts, ['urns_0', 'uuid'])
    contact_uuids = contact_uuids.rename( columns = {'urns_0': 'phone'} )

    df = df.merge(contact_uuids, how = 'left', on = 'phone')

    df.fillna('', inplace = True)

    return df


def add_groups(contact_uuids, group, action = 'add'):
    '''
        contact_uuids is a list of contact UUIDS to add.
        group is a string, the name of the group.
        Notice that RP has a 100 limit on number of contact_uuids to add to a group in each request.
    '''

    batch = []
    while len(contact_uuids) > 100:
        batch.append(contact_uuids[:100])
        contact_uuids = contact_uuids[100:]
    batch.append(contact_uuids[:])

    for l in batch:
        print(len(l))
        requests.post(
                'https://rapidpro.io/api/v1/contact_actions.json',
                headers = { 'content-type': 'application/json',
                            'Authorization': rp_api },
                data = json.dumps( { 'contacts': l,
                                     'action': action,
                                     'group': group } )
                     )
    return None


def remove_groups(contact_uuids, group):
    '''
        Wrapper on add_groups(). Removes contacts from specified group.
    '''

    add_groups(contact_uuids, group, action = 'remove')

    return None


def start_run(contact_uuids, flow):
    '''
        flow is a string e.g. 'miAlta_init'. It's the name of the flow to start the contact_uuids in.
    '''
    
    # Load flows dataset
    flows_df = io(root + raw_flows)

    # Get flow uuid
    flow_uuid = flows_df.loc[ (flows_df['name'] == flow), 'uuid'].values[0]
    print('Flow UUID is: ' + str(flow_uuid))

    batch = []
    while len(contact_uuids) > 100:
        batch.append(contact_uuids[:100])
        contact_uuids = contact_uuids[100:]
    batch.append(contact_uuids[:])

    for l in batch:
        print(len(l))
        requests.post( 'https://api.rapidpro.io/api/v1/runs.json',
              headers = {'content-type': 'application/json',
                         'Authorization': rp_api},
              data = json.dumps( { 'flow_uuid': flow_uuid,
                                   'contacts': l,
                                   'restart_participants': True } ) )
