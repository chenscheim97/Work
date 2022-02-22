##########################################
# this script purpose is alternation of Dans script which retrives the airflow status
# Date - 15/2/2022
# Writer - Chen Scheim / Dan Sever
# To - Dana Gindes the queen

############## INSTRUCTIONS  #############
# 1. start VPN
# 2. change SHEET_NAME to the required sheet in spreadsheet
# 3. make sure those files are in folder
#    - client_secret_800484712991-uiqs4l3dm4ti9u873h3eu1ap53ah7dnl.apps.googleusercontent.com.json
#    - token.pickle
# 4. make sure the required spreadsheet url is fixed in SPREADSHEET variable
# 5. run
##########################################


# IMPORTS
import requests
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os


# CONSTANTS
TXT_FILE_PATH = "dag_run_status.txt"
AIRFLOW_REQUEST_URI = "https://airflow.vr-int.cloud/api/v1/dags/Eclipse/dagRuns/{}"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET = 'https://docs.google.com/spreadsheets/d/1RsXZ5V7vMlouL6Yr5zM7wRPIMk2mV_Ql94B9q56C648/edit?usp=sharing'
SHEET_NAME = 'oprtun'
SAMPLE_SPREADSHEET_ID_input = SPREADSHEET.split('/')[5]
SAMPLE_RANGE_NAME = '{}!A1:AA1000'.format(SHEET_NAME)


# FUNCTIONS
def get_spreadsheet():
    """
    this script collects data from the spreadsheet

    I assisted this link:
    https://gist.github.com/prafuld3/920b33f036aef145c3135fb13baba80d
    :return: matrix
    """
    global values_input, service
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret_800484712991-uiqs4l3dm4ti9u873h3eu1ap53ah7dnl.apps.googleusercontent.com.json',
                SCOPES)  # here enter the name of your downloaded JSON file
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    return sheet


def get_data(sheet):
    """
    this script gathers the data from the spreadsheet
    :param sheet: the spreadsheet
    :return: matrix
    """
    result_input = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
                                      range=SAMPLE_RANGE_NAME).execute()
    return result_input["values"]


def get_dag_run(dag_run_id):
    """
    connecting to airflow to get the run status
    :param dag_run_id: dag_run_id of company we want to get the status of
    :return: response from airflow API with current status ('200' or '400')
    """
    final_url = AIRFLOW_REQUEST_URI.format(dag_run_id)
    response = requests.get(final_url, auth=("admin", "admin"))
    return response


def company_name_fixer(name):
    """
    fixs the name if ends with /n
    :param name: string
    :return: string
    """
    if name.endswith('\n'):
        return name.replace('\n', '')
    return name


def get_eclipse_status(data):
    """
    this runs throgh sheet companies and dag ids and looks for its airflow
    :param data: sheet from excel
    :return: dictionary with all company names and their respective status
    """
    company_names = [i[0] for i in data]  # all companies in sheet
    run_ids = [i[7] for i in data]  # all dag_run_ids in sheet
    id_status_dict = {}

    for i in range(len(company_names)):
        # assign dag_run_id to each company individually
        current_dag_run_id = run_ids[i]
        # get status from airflow using get_dag_run() method.
        status = get_dag_run(current_dag_run_id)
        # insert {company_name : company_status} to dictionary
        id_status_dict[company_name_fixer(company_names[i])] = status
    return id_status_dict


def make_txt_file(id_status_dict):
    """
    method creates a txt file,example:
        "company_a : 200"
        "company_b : 400"
        "company_c : 200", etc.
    :param id_status_dict: dictionary matching company to status
    :return: None
    """
    with open(TXT_FILE_PATH, 'w') as f:
        for company in id_status_dict:
            f.write(rf"{company} : {id_status_dict[company]}")
            f.write('\n')


def main():
    sheet = get_spreadsheet()
    data = get_data(sheet)[1:]  # matrix of all data from spreadsheet
    result_dict = {}
    if data[0]:
        result_dict = get_eclipse_status(data)
    make_txt_file(result_dict)


if __name__ == '__main__':
    main()
