# -*- coding: utf-8 -*-

# This file contains functions to upload all information to rapidpro
# coming from a processed PD dataset (PD1, PD2, PD3, PD4).

import json
import requests
import csv
import datetime
import configparser
import numpy as np
import pandas as pd

# Authentication
config = configparser.ConfigParser()
config.read('keys.ini')
# root path
root = config['paths']['root']
# RapidPro API token
rp_api = config['rapidpro']['rp_api']



class PD(object):
    '''
        Base class for all other PD classes.
    '''


    def __init__(self):
        '''
            Set directory locations for the clinic and locality-level master datasets, as well as
            PD datasets and RP contacts.
        '''

        self.clinicDb = 'processed/sample/cl_smp_20160314.csv'
        self.localityDb = 'processed/sample/loc_smp_20160320.csv'
        self.contacts = 'pRaw/rapidpro/contacts/contacts.csv'
        self.pd2Master = 'pProcessed/PD/PD2/pd2.csv'
        self.pdMaster = 'pProcessed/PD/master_pd.csv'
        self.flows = 'pRaw/rapidpro/flows/flows.csv'
    
    
    def io(self, dbPath, subset = None):
        '''
            Reads a dataset into dataframe, all string, np.nan set to ''.
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
            print(col, type(df[col].iloc[3]))

        return df
    
    
    def get_uuids(self, df):
        '''
            retrieves the contacts' uuids. The merge is on phone number
        '''
        
        # Call contacts
        contacts = self.io(self.contacts, ['urns_0', 'uuid'])
        contacts = contacts.rename( columns = {'urns_0': 'phone'} )
        
        df = df.merge(contacts, how = 'left', on = 'phone')

        df.fillna('', inplace = True)

        return df


    def vars_clinic(self):
        '''
            returns varnames association as a dict: {RP contactfield: dataset varname}
        '''
        
        return { 'clues': 'ext_clues',
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
                 'cl_loc_nombre_clCat': 'ext_cl_loc_nombre_clcat' }


    def get_clInfo(self, df):
        '''
            Merges the PD dataset with clinic master dataset and extracts all info to fill in 
            PD().vars_clinic(self)
            pd is a string that specifies the PD format. {'pd1', 'pd2', 'pd3', 'pd4'}
        '''
        

        # I/O: clinic master
        df_cl = self.io(self.clinicDb, 
                        ['clues',
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
                         'cl_latitude'])

        # merge on clues. I've checked all data types are preserved.
        df = df.merge(df_cl, how = 'left', on = 'clues')

        # fill in np.nan
        df.fillna('', inplace = True)

        return df


    def get_phInfo(self, df, pd):
        '''
            Merges the PD dataset on folio with appended PD2 and extracts phone number
            pd is a string that specifies the PD format. {'pd1', 'pd3', 'pd4'}
        '''

        # I/O: clinic master
        df_ph = self.io( self.pd2Master,
                         ['folio_pd2',
                          'phone'] )

        # Rename merging var: clues
        df = df.rename( columns = {'folio_' + pd: 'folio'} )
        df_ph = df_ph.rename( columns = {'folio_pd2': 'folio'} )

        # merge on folio
        df = df.merge(df_ph, how = 'left', on = 'folio', indicator = True)
        #print(df.loc[df['_merge'] == 'left_only'])
        df = df.drop('_merge', 1)

        # fill in np.nan
        df.fillna('', inplace = True)

        # Go back to name of folio column
        df = df.rename( columns = {'folio': 'folio_' + pd} )

        return df


    def vars_locality(self):
        '''
            returns dict {contact_field: value of nth row of dataset}
            df is a dataframe with locality information, at the individual level.
            n is the row of df.
        '''

        return { 'loc_latitud_geo': 'ext_loc_latitud_geo',
                 'loc_longitud_geo': 'ext_loc_longitud_geo',
                 'loc_nom_ent_cs10': 'ext_loc_nom_ent_cs10',
                 'loc_nom_mun_cs10': 'ext_loc_nom_mun_cs10',
                 'loc_nom_loc_cs10': 'ext_loc_nom_loc_cs10',
                 'loc_longitud_cs10': 'ext_loc_longitud_cs10',
                 'loc_latitud_cs10': 'ext_loc_latitud_cs10' }


    def get_locInfo(self, df):
        '''
            Merges the PD dataset with locality master dataset and extracts all info to fill in 
            PD().vars_locality(self)
            pd is a string that specifies the PD format. {'pd1', 'pd2', 'pd3', 'pd4'}
        '''
        

        # I/O: locality master
        df_loc = self.io(self.localityDb, 
                        [ 'id',
                          'loc_latitud_geo',
                          'loc_longitud_geo',
                          'loc_nom_ent_cs10',
                          'loc_nom_mun_cs10',
                          'loc_nom_loc_cs10',
                          'loc_longitud_cs10',
                          'loc_latitud_cs10' ])

        # merge on clues. I've checked all data types are preserved.
        df = df.merge(df_loc, how = 'left', on = 'id')

        # fill in np.nan
        df.fillna('', inplace = True)

        return df


    def update_fields(self, df, variables):
        '''
            Runs post requests with all available data.
            variables is a dict that contains the PD-RP correspondence of variable names and 
            contact fields we wish to update: a combination of vars_clinic, vars_locality and
            vars_pd`i', `i' \in {1, 2, 3, 4}.
        '''

        for row in range(len(df.index)):
            # Assemble contact fields to update
            to_update = {}

            for field in variables.keys():
                if ( df[field].iloc[row] != '' ) :
                    to_update[variables[field]] = df[field].iloc[row]
                else:
                    pass
            #to_update = {'ext-incorporate-date': '29/03/2016'}
            if to_update != {}:
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


    def add_groups(self, df):
        '''
            Adds contacts to the corresponding group in RP.
            - NOT3
            - T1 - T3
        '''
        
        def request(contacts, group):
            '''
                abstracts details of add to groups post request.
                contacts is a list of contacts to add.
                group is a string, the name of the group.
                Notice that RP has a 100 limit on number of contacts to add to a group.
            '''
            batch = []
            while len(contacts) > 100:
                batch.append(contacts[:100])
                contacts = contacts[100:]
            batch.append(contacts[:])

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
        
        # Add everyone to group ALL
        uuids = list(df.loc[df['uuid'] != '', 'uuid'])
        request(uuids, 'ALL')

        # Treatment arm grouping
        t1_uuids = df.loc[df['cl_treatmentArm'] == '1', 'uuid']
        t1_uuids = list(t1_uuids[t1_uuids != ''])
        request(t1_uuids, 'T1')
        request(t1_uuids, 'NOT3')

        t2_uuids = df.loc[df['cl_treatmentArm'] == '2', 'uuid']
        t2_uuids = list(t2_uuids[t2_uuids != ''])
        request(t2_uuids, 'T2')
        request(t2_uuids, 'NOT3')

        t3_uuids = df.loc[df['cl_treatmentArm'] == '3', 'uuid']
        t3_uuids = list(t3_uuids[t3_uuids != ''])
        request(t3_uuids, 'T3')

        return None


    def start_run(self, df, flow):
        '''
            flow is a string e.g. 'miAlta_init'
        '''
        
        # Load flows dataset
        flows_df = self.io(self.flows)

        # Get flow uuid
        flow_uuid = flows_df.loc[ (flows_df['name'] == flow), 'uuid'].values[0]

        # Get uuids list
        uuids = list(df.loc[df['uuid'] != '', 'uuid'])
        print(uuids)
        print(flow_uuid)

        requests.post( 'https://api.rapidpro.io/api/v1/runs.json',
              headers = {'content-type': 'application/json',
                         'Authorization': rp_api},
              data = json.dumps( { 'flow_uuid': flow_uuid,
                                   'contacts': uuids,
                                   'restart_participants': True } ) )




class PD1(PD):
    '''
        Inherited class for info specific to PD1 forms.
    '''

    
    def __init__(self, pd1Db):

        PD.__init__(self)
        self.pd1Db = pd1Db
        
    
    def vars_pd1(self):
        '''
            returns varnames association as a dict: {dataset varname: RP contactfield}
        '''

        return { 'folio': 'ext_folio',
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
                 'appt_visits_pd1': 'pd1_appt_visits' }


    def wrap(self):
        '''
            Calls and processes datasets, updates contact fields and groups according to treatment
            arm.
        '''

        df = self.io(self.pd1Db)
        df = self.get_phInfo(df, 'pd1')
        df = self.get_clInfo(df, 'pd1')

        variables = self.vars_clinic()
        variables.update(self.vars_pd1())
        self.update_fields(df, variables)

        df = self.get_uuids(df)
        self.add_groups(df)



class PD2(PD):
    '''
        Inherited class for info specific to PD2 forms.
    '''


    def __init__(self, pd2Db):

        PD.__init__(self)
        self.pd2Db = pd2Db
        

    def vars_pd2(self):
        '''
            returns varnames association as a dict: {dataset varname: RP contactfield}
        '''
        
        return { 'name_pd2': 'ext_name',
                 'nameF_pd2': 'ext_namef',
                 'nameM_pd2': 'ext_namem' ,
                 'birthday_pd2': 'ext_birthday',
                 'imei_pd2': 'pd2_imei' }


    def wrap(self):
        '''
            Calls and processes datasets, updates contact fields and groups according to treatment
            arm.
        '''

        df = self.io(self.pd2Db)
        df = self.get_clInfo(df, 'pd2')

        variables = self.vars_clinic()
        variables.update(self.vars_pd2())
        self.update_fields(df, variables)

        df = self.get_uuids(df)
        self.add_groups(df)


class PD3(PD):
    '''
        Inherited class for info specific to PD3 forms.
    '''

    def __init__(self, pd3Db = 'pProcessed/PD/PD3/pd3.csv'):

        PD.__init__(self)
        self.pd3Db = pd3Db


    def vars_pd3(self):
        '''
            returns varnames association as a dict: {dataset varname: RP contactfield}
        '''
        
        return { 'name_pd3': 'ext_name',
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
                 'loc4_loc_name_pd3': 'pd3_loc4_loc_name' }


    def add_auxvo(self, df):
        '''
            Adds vocales and auxiliares to  contacts to the corresponding group in RP.
            - NOT3
            - T1 - T3
        '''
        
        uuids = list(df.loc[:, 'uuid'])
        uuids_clean = [uuid for uuid in uuids if len(uuid) > 0]
        print(uuids_clean)

        requests.post(
                'https://rapidpro.io/api/v1/contact_actions.json',
                headers = { 'content-type': 'application/json',
                            'Authorization': rp_api },
                data = json.dumps( { 'contacts': uuids_clean,
                                     'action': 'add',
                                     'group': 'AUXVO' } )
                     )
        return None


    def wrap(self):

        df = self.io(self.pd3Db)
        df = self.get_clInfo(df)

        variables = self.vars_clinic()
        variables.update(self.vars_pd3())
        #self.update_fields(df, variables)
        
        df = self.get_uuids(df)
        self.add_groups(df)
        self.add_auxvo(df)


class PD4(PD):
    '''
        Inherited class for info specific to PD4 forms.
    '''

    def __init__(self, pd4Db):

        PD.__init__(self)
        self.pd4Db = pd4Db


    def vars_pd4(self):
        '''
            returns varnames association as a dict: {dataset varname: RP contactfield}
        '''
        
        return { 'promise_pd4': 'pd4_promise' }

    def wrap(self):

        df = self.io(self.pd4Db)
        df = self.get_clInfo(df, 'pd4')
        # WARNING: watch out for non-matched folios
        df = self.get_phInfo(df, 'pd4')

        variables = self.vars_clinic()
        variables.update(self.vars_pd4())
        #self.update_fields(df, variables)

        df = self.get_uuids(df)
        self.add_groups(df)


inst = PD()
instPD1 = PD1('')
instPD2 = PD2('')
instPD4 = PD4('')

df = inst.io(inst.pdMaster)
df = inst.get_clInfo(df)
df = inst.get_locInfo(df)

variables = inst.vars_clinic()
variables.update(inst.vars_locality())
variables.update(instPD1.vars_pd1())
variables.update(instPD2.vars_pd2())
variables.update(instPD4.vars_pd4())

print(df)
print(variables)
#inst.update_fields(df, variables)
df = inst.get_uuids(df)
#inst.add_groups(df)
inst.start_run(df, 'setApptDate')

#ans = input('proceed? (y/n)')
#if ans == 'y':
#    instPD3 = PD3('pProcessed/PD/PD3/pd3.csv')
#    instPD3.wrap()
#
#
## UI
#pd_number = input('Escribe el número de formato PD a procesar (e.g. 1, 2, 3, 4): ')
#print('Escribe el path a la base.')
#db = input('Usa / en lugar de \. e.g. pProcessed/PD/PD2/puebla/puebla.csv : ')
#confirm = input('¿Seguro que deseas continuar? Esto procederá con la incorporación a RP (y/n): ')
#
#if confirm == 'y':
#    if pd_number == '1':
#        inst = PD1(db)
#    elif pd_number == '2':
#        inst = PD2(db)
#    elif pd_number == '3':
#        inst = PD3(db)
#    elif pd_number == '4':
#        inst = PD4(db)
#    else:
#        raise NameError('No escribiste bien el número de formato PD a procesar')
#    
#    inst.wrap()
#
#elif confirm == 'n':
#    print('De acuerdo. ¡Hasta la próxima camarada!')
#
#else:
#    raise NameError('Había que responder con y o con n. Corre el script nuevamente')
#
#
########### CEMETERY ##########
##inst = PD()
##instPD1 = PD1('')
##instPD2 = PD2('')
##instPD4 = PD4('')
##
##df = inst.io(inst.pdMaster)
##variables = {'id': 'ext_loc_id'}
##inst.update_fields(df, variables)
##df = inst.get_clInfo(df)
##df = inst.get_locInfo(df)
##variables = inst.vars_clinic()
##variables.update(inst.vars_locality())
##variables.update(instPD1.vars_pd1())
##variables.update(instPD2.vars_pd2())
##variables.update(instPD4.vars_pd4())
##
##print(df)
##print(variables)
###ans = input('proceed? (y/n)')
###if ans == 'y':
###    #inst.update_fields(df, variables)
###    df = inst.get_uuids(df)
###    inst.add_groups(df)
##
##ans = input('proceed? (y/n)')
##if ans == 'y':
##    instPD3 = PD3('pProcessed/PD/PD3/pd3.csv')
##    instPD3.wrap()
