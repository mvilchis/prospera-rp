#CODIGO PARA SUBIR CONTACTOS

# Set path to get module
import sys
sys.path.append("/Users/Ana1/Dropbox/DropboxQFPD/pTasks/rapidpro")
#sys.path.append("/home/mvilchis/Desktop/Presidencia/Prospera/rp-pd/")

import os
import configparser
import utils
import get
#from repo.download import get
import pandas as pd
import requests
import json
import datetime as dt

# further configuration
config = configparser.ConfigParser()
# Get parent directory
#direct = os.path.dirname(os.path.dirname(__file__))
#config.read(direct + '/keys.ini')
config.read('keys.ini')
## Paths
root = config['paths']['root']
clinicDb = config['paths']['clinicDb']
localityDb = config['paths']['localityDb']
pdMaster = config['paths']['pdMaster']
pd3Master = config['paths']['pd3Master']
raw_contacts = config['paths']['raw_contacts']
last_contacts = config['paths']['last_contacts']
rp_api = config['rapidpro']['rp_api']


## GSheet url
HR_url = config['google']['HR_url']
HR_url2 = config['google']['HR_url2']
PD2_url = config['google']['PD2_url']
PD2_url2 = config['google']['PD2_url2']
FANTASMA_url = config['google']['FANTASMA_url']



class HR(object):
    '''
        This class defines methods to use the utils uploading infor that would
        be uploaded with miAlta
    '''
    def wrap_update(self, date, id_sheet=1):
        '''
            1) Loads gspread HR_url, 2) uses date DD/MM/YYYY to filter date and
            uploads, 3) after that performs tasks that otherwise would be done
            with miAlta
        '''
        id_sheet = id_sheet -1
        #1
        df = utils.read_gspread(HR_url2, id_sheet)
        for col in ['rp_duedate', 'rp_apptdate']:
            df.loc[df[col] == 'a punto', col] = ''
            df.loc[df[col].str.len() > 10, col] = ''

        for col in ['phone']:
            df[col] = 'tel:+52' + df[col].str.replace(' ', '')
            df.loc[df[col] == 'tel:+52', col] = ''

        df = df.loc[df['date'] == date, :]
        print "*"*50
        print "Hay %d contactos a actualizar" %(df['phone'].count())
        print "*"*50 + "\n"

        #1.1
        clinics = utils.io(root+clinicDb , ['clues','cl_treatmentArm'])
        clinics = clinics.rename(columns={'clues': 'ext_clues',
                                          'cl_treatmentArm':
                                          'ext_cl_treatmentarm'})
        df = pd.merge(df, clinics, how='left', on=['ext_clues'])

        #2
        utils.update_fields(df, ['rp_name',
                                 'rp_duedate',
                                 'rp_ispregnant',
                                 'rp_isaux_decl',
                                 'rp_isvocal_decl',
                                 'rp_prosperapal',
                                 'rp_apptdate',
                                 'ext_clues',
                                 'ext_cl_treatmentarm'], date)

        #3 Get uuid
        df['phone']=df['phone'].str[4:]

        #Get updated contacts uuids
        #mi_date=dt.datetime(int(date[-4:]), int(date[3:5]), int(date[:2]) )
        mi_date=dt.date.today()
        aux=(mi_date+dt.timedelta(1)).strftime('%Y-%m-%d')
        mi_date=mi_date.strftime('%Y-%m-%d')
        lond='T06:00:00.000Z'



        inst = get.GetContacts()
        #inst.export_contacts(parameters={'before': (aux+lond) , 'after':
        #                                 (mi_date+lond)},
        #                     path=root+last_contacts)
        print "*"*50
        print "Descargando los contactos de rapidpro para exportarlos a csv en: \n%s" %(root+last_contacts)
        inst.export_contacts(path=root+last_contacts)
        print "*"*50 + "\n"

        contacts = utils.io(root + last_contacts, ['phone', 'uuid'])
        print "*"*50
        print "Los contactos leidos de %s son %d" %(root + last_contacts, contacts.size)
        print "\n Primeros 5 \n%s" %(contacts.head(5))
        print "\n Ultimos 5  \n%s \n\n " %(contacts.tail(5))
        df = pd.merge(df, contacts, how='left', on=['phone'] )

        #3
        uuids_all = list(df['uuid'])
        print "Contactos a modificar: "
        print(df['rp_name'])
        print "*"*50 + "\n"

        print "*"*50

        # Register contacts to altas remotas
        #utils.add_groups(uuids_all, 'ALTAREMOTA')

        uuids_pregnant = list(df.loc[(df['rp_ispregnant'] == '1'), 'uuid'])
        utils.add_groups(uuids_pregnant, 'PREGNANT')

        uuids_spill = list(df.loc[ (df['rp_prosperapal'] == '0') &
                                   (df['rp_isaux_decl'] == '0') &
                                   (df['rp_isvocal_decl'] == '0') &
                                   (df['rp_ispregnant']=='1') ,
                                   'uuid' ])
        utils.add_groups(uuids_spill, 'spillovers')

        uuids_NotT3 = list(df.loc[(df['ext_cl_treatmentarm']=='1') |
                                  (df['ext_cl_treatmentarm']=='2') ,'uuid' ])
        utils.add_groups(uuids_NotT3, 'NOT3')

        uuids_T1 = list(df.loc[(df['ext_cl_treatmentarm']=='1'), 'uuid'])
        utils.add_groups(uuids_T1, 'T1')
        uuids_T2 = list(df.loc[(df['ext_cl_treatmentarm']=='2'), 'uuid'])
        utils.add_groups(uuids_T2, 'T2')
        uuids_T3 = list(df.loc[(df['ext_cl_treatmentarm']=='3'), 'uuid'])
        utils.add_groups(uuids_T3, 'T3')

        ###4
        #utils.start_run(uuids_pregnant, 'setApptDate_hr')

        return None


class PD2(object):
    '''
        This class defines methods to use the utils uploading infor that would
        be uploaded after capturing information from PD2
    '''
    def wrap_update(self, date):
        '''
            1) Loads gspread PD2_url, 2) after that performs tasks that otherwise would be done
            with miAlta
        '''
        #1
        df = utils.read_gspread(PD2_url)

        for col in ['phone']:
            df[col] = 'tel:+52' + df[col].str.replace(' ', '')
            df.loc[df[col] == 'tel:+52', col] = ''

        #1.1
        clinics = utils.io(root+clinicDb , ['clues','cl_treatmentArm'])
        clinics = clinics.rename(columns={'clues': 'ext_clues',
                                          'cl_treatmentArm':
                                          'ext_cl_treatmentarm'})
        df = pd.merge(df, clinics, how='left', on=['ext_clues'])
        print(df)

        #2
        utils.update_fields(df, ['rp_name',
                                 'ext_clues',
                                 'ext_folio',
                                 'ext_cl_treatmentarm'], date)

        # Place contacts in its treatment arm
        inst = get.GetContacts()
        inst.export_contacts(path=root+last_contacts)

        contacts = utils.io(root + last_contacts, ['phone', 'uuid'])
        print(contacts)
        df = pd.merge(df, contacts, how='left', on=['phone'] )

        uuids_T1 = list(df.loc[(df['ext_cl_treatmentarm']=='1'), 'uuid'])
        utils.add_groups(uuids_T1, 'T1')
        uuids_T2 = list(df.loc[(df['ext_cl_treatmentarm']=='2'), 'uuid'])
        utils.add_groups(uuids_T2, 'T2')
        uuids_T3 = list(df.loc[(df['ext_cl_treatmentarm']=='3'), 'uuid'])
        utils.add_groups(uuids_T3, 'T3')


class FANTASMA(object):
    '''
        This class defines methods to use the utils uploading infor that would
        be uploaded after capturing information from PD2
    '''
    def wrap_update(self, date):
        '''
            1) Loads gspread FANTASMA_url and transforms phone, 2) merges
            FANTASMA and contacts 3) updates the fields 4) sends to respective
            groups
        '''
        #1 reading gspread and transforming phone for merge
        df = utils.read_gspread(FANTASMA_url)

        # preparing phone format for merge with contacts
        df['phone'] = '+52' + df['phone'].str.replace(' ', '')

        #2 merge with contacts
        #inst = get.GetContacts()
        #inst.export_contacts(path=root+last_contacts)
        contacts = utils.io(root + last_contacts, ['phone', 'uuid'])
        print(contacts)
        df = pd.merge(df, contacts, how='left', on=['phone'] )
        print(df)

        #3 updating fields
        df['phone'] = 'tel:' + df['phone']

        # updating variables for contacts
        utils.update_fields(df, ['rp_ispregnant', 'rp_isvocal_decl', 'rp_duedate', 'rp_auxBB' ], date)

        #4 placing contacts in their groups
        uuids_all = list(df['uuid'])
        print(uuids_all)
        utils.add_groups(uuids_all, 'ALL')

        uuids_pregnant = list(df.loc[df['rp_ispregnant'] == '1', 'uuid'])
        utils.add_groups(uuids_pregnant, 'PREGNANT')

        uuids_puerperium = list(df.loc[df['rp_ispuerperium'] == '1', 'uuid'])
        utils.add_groups(uuids_puerperium, 'PUERPERIUM')


class RESCATE_T3(object):
    '''
        This class defines methods to use the utils uploading infor that would
        be uploaded after capturing information from PD2
    '''
    def wrap_update(self, date):
        '''
            1) Loads gspread FANTASMA_url and transforms phone, 2) merges
            FANTASMA and contacts 3) updates the fields 4) sends to respective
            groups
        '''
        #1 reading gspread and transforming phone for merge
        df = utils.read_gspread(FANTASMA_url)

        # preparing phone format for merge with contacts
        df['phone'] = '+' + df['phone'].str.replace(' ', '')

        #2 merge with contacts
        #inst = get.GetContacts()
        #inst.export_contacts(path=root+last_contacts)
        contacts = utils.io(root + last_contacts, ['phone', 'uuid'])
        print(contacts)
        df = pd.merge(df, contacts, how='left', on=['phone'] )
        print(df)

        #3 updating fields
        df['phone'] = 'tel:' + df['phone']

        # updating variables for contacts
        #utils.update_fields(df, ['rp_inc_info5'], date)

        #4 placing contacts in their groups
        uuids_all = list(df['uuid'])
        print(uuids_all)
        #utils.start_run(uuids_all, 'IncentivesCollectBabies')
        utils.start_run(uuids_all, 'incentivesCollect5')
        utils.add_groups(uuids_all, 'T3')
        utils.remove_groups(uuids_all,'NOT3')
        utils.remove_groups(uuids_all,'T2')
        utils.remove_groups(uuids_all,'T1')
