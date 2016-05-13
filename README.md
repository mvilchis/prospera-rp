# rp-pd
Toolbox for bulk operation around the RapidPro API (see https://rapidpro.io/api/v1/explorer/)

You should have a similar directory structure:

     parent/
          rp-pd/
               download/
                       get.py
               post/
                   utils.py
               keys.ini
          datasets/
                  GSheet/
                        getGSheet-<your credentials>.json

Sample keys.ini:

     [google]
     credentials = datasets/GSheet/getGSheet-<your credentials>.json

     [paths]
     root = /path/to/parent
     raw_contacts = datasets/contacts.csv
     raw_flows = datasets/flows.csv
     raw_fields = datasets/fields.csv
     raw_groups = datasets/groups.csv
     raw_messages = datasets/messages.csv
     raw_runs = datasets/
     
     [rapidpro]
     rp_api = Token 1pm12yp4uoig2jl34y2ptoio4jk23e24n

This should get you up and running.

download/get.py allows you to download and export in .csv format almost all datasets provided by the RapidPro API:
contacts, groups, fields, flows, messages and runs.
Any nested data structures are flattened before exporting to .csv.

Runs are treated aside because of their complex data structure and added a couple of useful variables: 
(i) chronological order of steps within a run (order);
(ii) flow name of run (flow_name);
(iii) run duration, in seconds (run_time);
(iv) step duration, in seconds (step_time);
(v) number of contact mistakes in step (mistakes)

Example use case (using IPython):

     In [1]: run get.py
     In [2]: inst = GetContacts()
     In [3]: inst.export_contacts()

These commands would download, flatten and export datasets/contacts.csv. The process to retrieve groups.csv, fields.csv, flows.csv and messages.csv is analogous.

Only retrieving runs.csv is a bit different:

     In [1]: run get.py
     In [2]: inst = ExportRuns()
     In [3]: inst.export_runs()

post/utils.py provides a set of tools to run selected RapidPro API post requests, with emphasis on integration with Google Spreadsheets.

It allows you to read an external dataset with contact information (such as a .csv or a Google Spreadsheet) and
(i) Update contacts' selected contact fields with user-specified columns
(ii) Add and remove contacts from groups
(iii) Start contacts in a flow

Example use case. Suppose my.csv looks like:

     "age", "phone"
     12, "tel:+525512345678"
     43, "tel:+521234567890"

And suppose that rp_age is a contact field key in RapidPro. Then the following would update (or create and update if contacts are not yet incorporated) rp_age with the values of column "age" for each of the contacts:

     In [1]: run utils.py
     In [2]: df = io('my.csv')
     In [3]: update_fields(df, {'age': 'rp_age'})

If my.csv is instead a Google Spreadsheet located in https://docs.google.com/spreadsheets/d/41234kllkerbwhlerkn8/edit#gid=0
then the following would execute the same procedure:

     In [1]: run utils.py
     In [2]: df = read_gspread('https://docs.google.com/spreadsheets/d/41234kllkerbwhlerkn8/edit#gid=0')
     In [3]: update_fields(df, {'age': 'rp_age'})

You ought to use OAuth2 for authorization to read the Google Spreadsheet (see http://gspread.readthedocs.io/en/latest/oauth2.html for more information). 
