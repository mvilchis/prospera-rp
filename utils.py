# -*- coding: utf-8 -*-

# This is the main file.
# It contains functions that miscellaneous tasks centered around the RapidPro
# API with a focus on interaction with external datasets.

import json
import requests
import csv
import datetime
import configparser
import numpy as np
import pandas as pd


# configuration
config = configparser.ConfigParser()
config.read('keys.ini')
## RapidPro API token
rp_api = config['rapidpro']['rp_api']
## Paths
root = config['paths']['root']
contacts = config['paths']['contacts']
flows = config['paths']['flows']




def io(dbPath, subset = None):
	'''
		Reads a .csv into dataframe, all string, np.nan set to ''.
		The encoding is set to latin-1 since I assume the dataset comes from STATA 13 (or <13)
			handling. These STATA versions use this encoding. Unicode is supported only from
			STATA 14 onwards...
		dbPath is the relative path to the dataset (starting at root, see beginning of file.)
		It's important to get everything as string: some integer cols are otherwise assigned
			a float type and when converted to string are displayed as floats...
	'''

	df = pd.read_csv(root + dbPath,
					 encoding= 'latin-1',
					 dtype = 'str',
					 usecols = subset)

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


def update_fields(df, variables, date=None):
	'''
		Runs post requests to update contact fields associated in variables.
		Missing data are ignored and requests are executed with all available information.
		variables is a dict that contains the correspondence of variable names in the current
		dataset (df) and RapidPro contact fields. e.g.
			{
				'var1': 'contact_field1',
			 	'var2': 'contact_field2'
			}
	date is today's date (to keep track of when things happened in RP) in format DD/MM/YYYYY
	'''

	if date == None:
		raise IOError("""You forgot to specify today's date! 
						 Please do so with format DD/MM/YYYY
						 (It is the last argument in function call)""")
	else:

		for row in range(len(df.index)):
			# Assemble contact fields to update
			to_update = {}

			for field in variables.keys():
				if df[field].iloc[row] != '' :
					to_update[variables[field]] = df[field].iloc[row]
				else:
					pass

			if to_update != {}:
				to_update['ext-incorporate-date'] = date
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
	contact_uuids = io(contacts, ['urns_0', 'uuid'])
	contact_uuids = contact_uuids.rename( columns = {'urns_0': 'phone'} )

	df = df.merge(contact_uuids, how = 'left', on = 'phone')

	df.fillna('', inplace = True)

	return df


def add_groups(contact_uuids, group):
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
									 'action': 'add',
									 'group': group } )
					 )
	return None


def start_run(contact_uuids, flow):
	'''
		flow is a string e.g. 'miAlta_init'. It's the name of the flow to start the contact_uuids in.
	'''
	
	# Load flows dataset
	flows_df = io(flows)

	# Get flow uuid
	flow_uuid = flows_df.loc[ (flows_df['name'] == flow), 'uuid'].values[0]

	requests.post( 'https://api.rapidpro.io/api/v1/runs.json',
		  headers = {'content-type': 'application/json',
					 'Authorization': rp_api},
		  data = json.dumps( { 'flow_uuid': flow_uuid,
							   'contacts': contact_uuids,
							   'restart_participants': True } ) )
