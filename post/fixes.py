# -*- coding: utf-8 -*-

# This file deals small with high level functions execute small fixes on RP contacts.
# It relies heavily on utils.py


import utils
import configparser
from datetime import timedelta
import numpy as np
import pandas as pd


# further configuration
config = configparser.ConfigParser()
config.read('keys.ini')
## Paths
contactsDb = config['paths']['contacts']


def mondayize():
    '''
        Sets the contact field rp_dueDate to be on the nearest monday. i.e.
            - Tuesday, Wednesday and Thursday go to previous Monday
            - Friday, Saturday and Sunday go to next Monday
    '''
    df = utils.io( contactsDb,
                   [ 'fields_rp_duedate',
                     'urns_0' ] )
    df = df.rename(columns={'urns_0': 'phone'})

    # Keep only the date part of string
    df['rp_duedate'] = df['fields_rp_duedate'].str[:10]

    # Convert rp_duedate to datetime
    df['rp_duedate'] = pd.to_datetime(df['rp_duedate'], dayfirst=True)

    # Add column with day of the week contained in rp_duedate
    df['dow'] = df['rp_duedate'].apply(lambda x: x.weekday())
    
    # Save the contacts' due date that do not happen on monday
    df['rp_duedate_notmonday'] = df.loc[df['dow'] != 0, 'rp_duedate']
    df['rp_duedate_notmonday'] = df['rp_duedate_notmonday'].dt.strftime('%d/%m/%Y')
    df.loc[df['rp_duedate_notmonday'] == 'NaT', 'rp_duedate_notmonday'] = ''
    utils.update_fields(df, {'rp_duedate_notmonday': 'rp_duedate_notmonday'})

    # Number of days to add or subtract
    df['add_days'] = df['dow']
    df.loc[df['dow'] < 4, 'add_days'] = -df['dow']
    df.loc[df['dow'] > 3, 'add_days'] = 7 - df['dow']

    # Proceed with addition/subtraction
    df['rp_duedate'] = df['rp_duedate'] + pd.TimedeltaIndex(df['add_days'], unit='D')

    # Go back to string
    df['rp_duedate'] = df['rp_duedate'].dt.strftime('%d/%m/%Y')
    df.loc[df['rp_duedate'] == 'NaT', 'rp_duedate'] = ''
    
    # drop cols
    df = df.drop([ 'fields_rp_duedate',
                   'rp_duedate_notmonday',
                   'dow',
                   'add_days' ],
                  axis=1)

    # Update contacts with new due date
    print(df)
    utils.update_fields(df, {'rp_duedate': 'rp_duedate'})

