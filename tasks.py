# -*- coding: utf-8 -*-

# This file deals with high level functions that process the PD datasets. Some of them rely on
# functions defined in utils.py
# The main dataset to have in mind is pd_master.csv added with RP uuids.

import configparser
import utils


# further configuration
config = configparser.ConfigParser()
config.read('keys.ini')
## Paths
clinicDb = config['paths']['clinicDb']
localityDb = config['paths']['localityDb']
pdMaster = config['paths']['pdMaster']
pd3Master = config['paths']['pd3Master']


def get_clInfo(df):
	'''
		Merges the PD dataset with clinic master dataset and extracts all clinic info to be passed
		on for contact field updates.
	'''

	# I/O: clinic master
	df_cl = utils.io(clinicDb, 
					 [ 'clues',
					   'cl_treatmentArm',
					   'cl_jur_nombre_clCat',
					   'cl_mun_nombre_clCat',
					   'cl_loc_nombre_clCat',
					   'cl_ent_nombre_clCat',
					   'cl_inst_clCat',
					   'cl_tipo_nombre_clCat',
					   'cl_nombre_clCat',
					   'cl_domicilio_clCat',
					   'cl_cp_clCat',
					   'cl_longitude',
					   'cl_latitude' ] )

	# merge on clues. All data types must be preserved (string).
	df = df.merge(df_cl, how = 'left', on = 'clues')

	# fill in np.nan
	df.fillna('', inplace = True)

	return df


def get_locInfo(df):
	'''
		Merges the PD dataset with clinic master dataset and extracts all locality info to be
		passed on for contact field updates.
	'''

	# I/O: locality master
	df_loc = utils.io(localityDb, 
					[ 'id',
					  'loc_latitud_geo',
					  'loc_longitud_geo',
					  'loc_nom_ent_cs10',
					  'loc_nom_mun_cs10',
					  'loc_nom_loc_cs10',
					  'loc_longitud_cs10',
					  'loc_latitud_cs10' ])

	# merge on INEGI locality ID. All data types must be preserved (string).
	df = df.merge(df_loc, how = 'left', on = 'id')

	# fill in np.nan
	df.fillna('', inplace = True)

	return df


def vars_clinic():
	'''
		returns varname-contactField association as a dict: {RP contactfield: dataset varname}
		this is for clinic-level vars.
	'''
	
	return {
				'clues': 'ext_clues',
				'cl_jur_nombre_clCat': 'ext_cl_jur_nombre_clcat',
				'cl_tipo_nombre_clCat': 'ext_cl_tipo_nombre_clcat',
				'cl_domicilio_clCat': 'ext_cl_domicilio_clcat',
				'cl_cp_clCat': 'ext_cl_cp_clcat',
				'cl_longitude': 'ext_cl_longitude',
				'cl_latitude': 'ext_cl_latitude',
				'cl_nombre_clCat': 'ext_cl_nombre_clcat',
				'cl_treatmentArm': 'ext_cl_treatmentarm',
				'cl_inst_clCat': 'ext_cl_inst_clcat',
				'cl_ent_nombre_clCat': 'ext_cl_ent_nombre_clcat',
				'cl_mun_nombre_clCat': 'ext_cl_mun_nombre_clcat',
				'cl_loc_nombre_clCat': 'ext_cl_loc_nombre_clcat'
		   }


def vars_locality():
	'''
		returns varname-contactField association as a dict: {RP contactfield: dataset varname}
		this is for locality-level vars.
	'''

	return {
				'loc_latitud_geo': 'ext_loc_latitud_geo',
				'loc_longitud_geo': 'ext_loc_longitud_geo',
				'loc_nom_ent_cs10': 'ext_loc_nom_ent_cs10',
				'loc_nom_mun_cs10': 'ext_loc_nom_mun_cs10',
				'loc_nom_loc_cs10': 'ext_loc_nom_loc_cs10',
				'loc_longitud_cs10': 'ext_loc_longitud_cs10',
				'loc_latitud_cs10': 'ext_loc_latitud_cs10'
		   }


def vars_pd1():
	'''
		returns varname-contactField association as a dict: {RP contactfield: dataset varname}
		this is for PD1 vars.
	'''

	return {
				'folio': 'ext_folio',
				'name_pd1': 'ext_name',
				'nameF_pd1': 'ext_namef',
				'nameM_pd1': 'ext_namem',
				'birthday_pd1': 'ext_birthday',
				'week_pd1': 'pd1_week',
				'dueDate_pd1': 'pd1_duedate',
				'risk_pd1': 'pd1_risk',
				'risk_cause_pd1': 'pd1_risk_cause',
				'complication_pd1': 'pd1_complication',
				'complication_txt_pd1': 'pd1_complication_txt',
				'complication_past_pd1': 'pd1_complication_past',
				'complication_past_txt_pd1': 'pd1_complication_past_txt',
				'complication_ext_pd1': 'pd1_complication_ext',
				'complication_ext_txt_pd1': 'pd1_complication_ext_txt',
				'pastPregnancies_pd1': 'pd1_pastpregnancies',
				'appt_nextDate_pd1': 'pd1_appt_nextdate',
				'wshpDate_pd1': 'pd1_wshpdate',
				'appt_visits_pd1': 'pd1_appt_visits'
		   }


def vars_pd2():
	'''
		returns varname-contactField association as a dict: {RP contactfield: dataset varname}
		this is for PD2 vars.
	'''
	
	return {
				'name_pd2': 'ext_name',
				'nameF_pd2': 'ext_namef',
				'nameM_pd2': 'ext_namem' ,
				'birthday_pd2': 'ext_birthday',
				'imei_pd2': 'pd2_imei'
		   }


def vars_pd3():
	'''
		returns varname-contactField association as a dict: {RP contactfield: dataset varname}
		this is for PD3 vars.
	'''
	
	return {
				'name_pd3': 'ext_name',
				'nameF_pd3': 'ext_namef',
				'nameM_pd3': 'ext_namem',
				'birthday_pd3': 'ext_birthday',
				'numLocs_pd3': 'pd3_numlocs',
				'loc1_inegi_pd3': 'pd3_loc1_inegi',
				'loc2_inegi_pd3': 'pd3_loc2_inegi',
				'loc3_inegi_pd3': 'pd3_loc3_inegi',
				'loc4_inegi_pd3': 'pd3_loc4_inegi',
				'loc1_state_name_pd3': 'pd3_loc1_state_name',
				'loc1_mun_name_pd3': 'pd3_loc1_mun_name',
				'loc1_loc_name_pd3': 'pd3_loc1_loc_name',
				'loc2_state_name_pd3': 'pd3_loc2_state_name',
				'loc2_mun_name_pd3': 'pd3_loc2_mun_name',
				'loc2_loc_name_pd3': 'pd3_loc2_loc_name',
				'loc3_state_name_pd3': 'pd3_loc3_state_name',
				'loc3_mun_name_pd3': 'pd3_loc3_mun_name',
				'loc3_loc_name_pd3': 'pd3_loc3_loc_name',
				'loc4_state_name_pd3': 'pd3_loc4_state_name',
				'loc4_mun_name_pd3': 'pd3_loc4_mun_name',
				'loc4_loc_name_pd3': 'pd3_loc4_loc_name'
		   }


def vars_pd4():
	'''
		returns varname-contactField association as a dict: {RP contactfield: dataset varname}
		this is for PD4 vars.
	'''
	
	return { 'promise_pd4': 'pd4_promise' }


def group_bf(df):
	'''
		Adds all contacts to ALL, treatment arm groups and NOT3 (for T1 and T2)
	'''
	
	# Add everyone to group ALL
	uuids = list(df.loc[df['uuid'] != '', 'uuid'])
	utils.add_groups(uuids, 'ALL')

	# Treatment arm grouping
	t1_uuids = df.loc[df['cl_treatmentArm'] == '1', 'uuid']
	t1_uuids = list(t1_uuids[t1_uuids != ''])
	utils.add_groups(t1_uuids, 'T1')
	utils.add_groups(t1_uuids, 'NOT3')

	t2_uuids = df.loc[df['cl_treatmentArm'] == '2', 'uuid']
	t2_uuids = list(t2_uuids[t2_uuids != ''])
	utils.add_groups(t2_uuids, 'T2')
	utils.add_groups(t2_uuids, 'NOT3')

	t3_uuids = df.loc[df['cl_treatmentArm'] == '3', 'uuid']
	t3_uuids = list(t3_uuids[t3_uuids != ''])
	utils.add_groups(t3_uuids, 'T3')

	return None


def group_auxvo(df_pd3):
	'''
		TODO: this seems like function overkill
		Adds vocales and auxiliares to contacts to the corresponding group in RP.
		- NOT3
		- T1 - T3
	'''
	
	uuids = list(df_pd3.loc[:, 'uuid'])
	uuids_clean = [uuid for uuid in uuids if len(uuid) > 0]
	print(uuids_clean)
	
	utils.add_groups(uuids_clean, 'ALL')
	utils.add_groups(uuids_clean, 'AUXVO')

	return None


def wrapper_pdMaster(date):
	'''
		date is in format DD/MM/YYYY
		Executes all procedures using PD master dataset to incorporate contacts to RapidPro.
		- Update contact fields with clinic, locality and individual information (PD{1, 2, 4})
		- Group contacts according to treatment arm, add all contacts to ALL, NOT3
	'''
	
	# Assemble dataset
	df = utils.io('pTasks/rapidpro/incorporate/repo/master_pd.csv')
	df = utils.get_uuids(df)
	df = get_locInfo(df)
	df = get_clInfo(df)
	
	# Assemble variable-field correspondence
	variables = vars_clinic()
	for dic in [ vars_locality(),
				 vars_pd1(),
				 vars_pd2(),
				 vars_pd4() ]:
		variables.update(dic)
	
	# Execute procedures
	utils.update_fields(df, variables, date)
	group_bf(df)
	# TO-DO: add contacts in T2 to vocal group (phone num)

	return None


def wrapper_pd3(date):
	'''
		date is in format DD/MM/YYYY
		Executes all procedures using PD3 master dataset to incorporate vocales/aux to RapidPro.
		- Update contact fields with clinic, locality and individual information (PD3)
		- Group contacts according to treatment arm, add all contacts to ALL, NOT3
	'''
	
	# Assemble dataset
	df = utils.io(pd3Master)
	df = utils.get_uuids(df)
	df = get_locInfo(df)
	df = get_clInfo(df)

	# Assemble variable-field correspondence
	variables = vars_clinic()
	for dic in [ vars_locality(),
				 vars_pd3() ]:
		variables.update(dic)
	
	# Execute procedures
	utils.update_fields(df, variables, date)
	group_auxvo(df)
