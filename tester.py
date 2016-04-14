# Tester for incorporate.py

#Take the following datasets:
#contacts
#pd_master
#flows

#on the contacts db, keep phones mine, manett, rodrigo, cristina
#on pd_master, invent data for same phones


import utils as i

here = 'pTasks/rapidpro/incorporate/repo/'

# test io
# contacts dataset
for db in [ here + 'contacts.csv',
			here + 'master_pd.csv',
			here + 'flows.csv' ]:
	df = i.io(db)
	print(df.head())

# test update_fields
df =  i.io(here + 'master_pd.csv')
i.update_fields(df,
				{ 'name_pd2': 'ext_name',
				  'nameF_pd2': 'ext_namef',
				  'nameM_pd2': 'ext_namem' ,
				  'birthday_pd2': 'ext_birthday',
				  'imei_pd2': 'pd2_imei' },
				'14/04/2016')

# test get_uuids
df = i.get_uuids(df)
print(df.head())

# test add_groups
i.add_groups(list(df.loc[df['uuid'] != '', 'uuid']), 'followUp_fdv')

# test start_run
i.start_run(list(df.loc[df['uuid'] != '', 'uuid']), 'temp')
