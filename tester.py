# Tester for incorporate.py

#Take the following datasets:
#contacts
#pd_master
#flows

#on the contacts db, keep phones mine, manett, rodrigo, cristina
#on pd_master, invent data for same phones


import incorporate as i

# test io
# contacts dataset
for db in [ 'contacts.csv',
			'pd_master.csv',
			'flows.csv' ]:
	df = i.io(db)
	print(df.head())

# test update_fields
df =  i.io('pd_master.csv')
i.update_fields(df,
				{ 'cl_cp_clCat': 'ext_cl_cp_clcat',
                  'cl_longitude': 'ext_cl_longitude',
                  'cl_latitude': 'ext_cl_latitude',
                  'cl_nombre_clCat': 'ext_cl_nombre_clcat',
                  'cl_treatmentArm': 'ext_cl_treatmentarm',
                  'cl_inst_clCat': 'ext_cl_inst_clcat',
                  'cl_ent_nombre_clCat': 'ext_cl_ent_nombre_clcat',
                  'cl_mun_nombre_clCat': 'ext_cl_mun_nombre_clcat',
                  'cl_loc_nombre_clCat': 'ext_cl_loc_nombre_clcat' })
i.update_fields(df,
				{ 'cl_cp_clCat': 'ext_cl_cp_clcat',
                  'cl_longitude': 'ext_cl_longitude',
                  'cl_latitude': 'ext_cl_latitude',
                  'cl_nombre_clCat': 'ext_cl_nombre_clcat',
                  'cl_treatmentArm': 'ext_cl_treatmentarm',
                  'cl_inst_clCat': 'ext_cl_inst_clcat',
                  'cl_ent_nombre_clCat': 'ext_cl_ent_nombre_clcat',
                  'cl_mun_nombre_clCat': 'ext_cl_mun_nombre_clcat',
                  'cl_loc_nombre_clCat': 'ext_cl_loc_nombre_clcat' },
				'13/04/2016')

# test get_uuids
df = i.get_uuids(df)
print(df.head())

# test add_groups
i.add_groups(list(df.loc[df['uuid'] != '', 'uuid']), 'followUp_fdv')

# test start_run
i.start_run(list(df.loc[df['uuid'] != '', 'uuid']), 'temp')
