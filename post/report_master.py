# -*- coding: utf-8 -*-

# This file executes all processes to report on current capacitaciones
# Specifically, it
#   - Updates runs dataset (download + processing)
#   - Updates contacts dataset
#   - Updates field reports dataset
#   - Runs tex report generation (STATA)


import subprocess
import os
import numpy as np
import pandas as pd
import datetime as dt
import get

#user= "/Users/Ana1/Dropbox/DropboxQFPD"
#user = "c: /users/francisco del villar/Dropbox (qfpd)/"
#user = "D:/Dropbox/DropboxQFPD"
user = "/home/mvilchis/Desktop/Presidencia/Prospera/rp-pd/"
download = user + "/pTasks/rapidpro/repo/download/"
runs_process = user + "/pProcessed/rapidpro/runs/runs_20160419.do"
runs_processR = user + "/pProcessed/rapidpro/runs/runs_20160504.R"
contacts_process = user + "/pProcessed/rapidpro/contacts/"
utilities = user + "/pTasks/rapidpro/repo/post"
report_dir = user + "/pTasks/rapidpro/report/capacitaciones"


def fetch_report(date):
    '''
        date is a string containing the date to extract from report spreadsheet.
        e.g. "06/05/2016"
    '''

    # Import reports dataset
    os.chdir(utilities)
    import utils
    df_chis = utils.read_gspread('https://docs.google.com/spreadsheets/d/130XIqnU5AZrG9CLrMdL3kkPVjTs3frvk7_ZO7dOFg98/edit#gid=0')
    df_edom = utils.read_gspread('https://docs.google.com/spreadsheets/d/1BobMLU4Q0PVClCUgNoLPWdtFeamYbH7QKnlIwFTrgow/edit#gid=0')

    df = df_chis.append(df_edom, ignore_index=True)

    # Go back to initial directory
    os.chdir(os.path.dirname(__file__))

    # Rename vars
    df = df.rename(columns = {
                                 "CLUES": "clues",
                                 "Empezaste MI ALTA? (1:SI, 0:NO)": "cap_mialta_started",
                                 "Empezaste MI PRUEBA? (1:SI , 0: NO)": "cap_miprueba_started",
                                 "Iniciales": "iniciales",
                                 "Lo pudieron resolver? (1:SI, 0:NO)": "cap_connect_pbs_resolved",
                                 "No lo pudieron resolver -> qué hicieron?": "connect_pbs_nRes_action",
                                 "No.": "No.",
                                 "Nombre de clínica": "Nombre de clínica",
                                 "Observaciones": "observaciones",
                                 "SI no, por qué?": "capacitacion_why",
                                 "Se hizo la capacitación? (1:SI , 0: NO)": "capacitacion",
                                 "Tuvieron problemas con la señal de celular? (1:SI, 0:NO)": "cap_connect_pbs",
                                 "fecha": "fecha",
                                 "num de beneficiarias que asistieron": "attendance",
                                 "num de beneficiarias que terminaron MI ALTA": "mialta_finished",
                                 "num de celulares entregados": "phones_delivered",
                                 "num de vocales/auxiliares que asistieron al taller": "attendance_vocaux",
                                 "num de vocales/auxiliares que se dieron de ALTA": "mialta_vocaux"
                             }
                  )
    print(df)

    # Keep observations from required date
    df = df.loc[df['fecha'] == date, :]

    # Remove unwanted characters
    chars = {
                'á': 'a',
                'é': 'e',
                'í': 'i',
                'ó': 'o',
                'ú': 'u',
                'ñ': 'n',
                '$': '',
                'ª': ''
            }
    for ugly in chars.keys():
        for col in ['observaciones',
                    'Nombre de clínica',
                    'connect_pbs_nRes_action']:
            df[col] = df[col].str.replace(ugly, chars[ugly], case=False)

    df.to_csv(report_dir + "/current_day.csv", index=False)

    return None


def export_missContacts(date):
    '''
        Gets contacts dataset, extracts contacts with missing info and saves them to a Google
        Spreadsheet.
        We wish to recover: rp_name,
                            rp_prosperaPal,
                            rp_isPregnant,
                            rp_isAux_decl,
                            rp_isVocal_decl,
                            rp_dueDate
                            rp_apptDate
        Vars to use: has_duedate; has_apptdate

        date is in format dd/mm/yyyy
    '''

    # Get contacts dataset (only vars listed above + phone + contact uuid)
    os.chdir(utilities)
    import utils
    df = utils.io( contacts_process + 'contacts.csv', 
                   [ 'fields_rp_name',
                     'fields_rp_prosperapal',
                     'fields_rp_ispregnant',
                     'fields_rp_isaux_decl',
                     'fields_rp_isvocal_decl',
                     'fields_rp_duedate',
                     'fields_rp_apptdate',
                     'has_duedate',
                     'has_apptdate',
                     'contact_created_on',
                     'phone',
                     'contact' ] )

    # Go back to initial directory
    os.chdir(os.path.dirname(__file__))

    # Keep observations s.t. has_duedate == 0 or has_apptdate == 0
    df = df.loc[ (df['has_duedate'] == '0') |
                 (df['has_apptdate'] == '0') , : ]
    
    # Get today and next day in %Y-%m-%d format
    date_given = dt.datetime.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d')
    date_next = dt.datetime.strptime(date, '%d/%m/%Y') + dt.timedelta(days=1)
    date_next = date_next.strftime('%Y-%m-%d')
    
    # Keep contacts that were created during this special date
    df['in_date'] = ( (df['contact_created_on'] > date_given + 'T06:00:00.000Z') &
                      (df['contact_created_on'] < date_next + 'T06:00:00.000Z') )
    df = df.loc[df['in_date'] == 1, :]

    # Keep observations with phone
    df = df.loc[df['phone'] != '', :]
    
    # Keep relevant columns
    df = df.loc[ :, ['fields_rp_name',
                     'fields_rp_prosperapal',
                     'fields_rp_ispregnant',
                     'fields_rp_isaux_decl',
                     'fields_rp_isvocal_decl',
                     'fields_rp_duedate',
                     'fields_rp_apptdate',
                     'phone',
                     'contact'] ]
    df['date'] = date

    # Assemble list of lists to write on spreadsheet
    ## Order columns
    df = df.sort_index(axis=1)
    names = df.columns.tolist()
    names = names[-1:] + names[1:-1] + names[0:1]
    df = df[names]
    data = []
    for row in range(len(df.index)):
        data.append(list(df.iloc[row]))
    
    print(data)
    # Write on spreadsheet
    os.chdir(utilities)
    import utils
    utils.rowAppend_gspread('https://docs.google.com/spreadsheets/d/1AdjVn9QoEDh4xb5Mgvt_wzgFCOTzmydqV06_01VvHzI/edit#gid=0', data) 

    # Go back to initial directory
    os.chdir(os.path.dirname(__file__))

    return None


def wrap(date):
    '''
        Executes procedures to generate report supposing that all datasets are there.
        date is a string containing the date to extract from report spreadsheet.
        e.g. "06/05/2016"
    '''

    # Fetch field reports
    fetch_report(date)

    #Cambio formato fecha
    mi_date=dt.datetime(int(date[-4:]), int(date[3:5]), int(date[:2]))
    aux=(mi_date+dt.timedelta(1)).strftime('%d-%m-%Y')
    mi_date=mi_date.strftime('%d-%m-%Y')
    lond='T06:00:00.000Z'

    # Create tex report
    cmd = ['StataSE', 'do', report_dir + '/report_activity.do',
           user] #mi_date, mi_date+lond, aux+lond]
    #cmd = ['StataMP-64', 'do', report_dir + '/report_activity.do', user]
    subprocess.call(cmd)

    # Get date as used in .tex
    texDate = date[6:] + '-' + date[3:5] + '-' + date[:2]
    print(texDate)

    # Change directory to the location of tex file
    os.chdir(report_dir)

    # Compile tex report
    cmd = ['pdflatex', 'reporte_capacitacion_' + texDate + '.tex']
    subprocess.call(cmd)

    # Go back to initial directory
    os.chdir(os.path.dirname(__file__))

    export_missContacts(date)

    return None

def wrap_full(date, isUpdate=None):
    '''
        Executes procedures to generate report using R scripts instead of do-files (except for
        report creation)
        date is a string containing the date to extract from report spreadsheet.
        e.g. "06/05/2016"
        isUpdate is boolean True/False to avoid unnecesary updating time
    '''
    if isUpdate:
        # Import get.py, instantiate ExportRuns and run export_runs(date)
        #os.chdir(download)

        # Import flows
        print('In flows...')
        flows = get.GetFlows()
        flows.export_flows()
        print('Out flows')

        # Now get those runs
        #print('In runs...')
        #runs = get.ExportRuns()
        #runs.export_runs()
        # runs.append_runs()
        #print('Out runs')

        # Now run export_contacts(date)
        print('In contacts...')
        contacts = get.GetContacts()
        contacts.export_contacts()
        print('Out contacts')
    else:
        print('Data already updated proceed with data manipulation')

    # Go back to initial directory
    os.chdir(os.path.dirname(__file__))

    # Process runs and contacts datasets
    #for cmd in (['Rscript', runs_processR, user],
                #['Rscript', contacts_process + 'contacts_20160504.R', user]):
        #subprocess.check_call(cmd)

    # Proceed
    # wrap(date)
