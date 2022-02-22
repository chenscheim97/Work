##########################################
# this script purpose is to convert table lines into eclips syntax
# Date - 15/2/2022
# Writer - Chen Scheim
# To - Dana Gindes the queen

############## INSTRUCTIONS  #############
# 1. start VPN
# 2. change SHEET_NAME to the required sheet in spreadsheet
# 3. make sure those files are in folder
#    - client_secret_800484712991-uiqs4l3dm4ti9u873h3eu1ap53ah7dnl.apps.googleusercontent.com.json
#    - token.pickle
# 4. make sure the required spreadsheet url is fixed in SPREADSHEET variable
# 5. run

################# REFS ###################
# https://console.developers.google.com/apis/api/sheets.googleapis.com/metrics?project=mindful-faculty-340810
##########################################


# IMPORTS
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os
import cli


# CONSTANTS
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SYNTAX = r'eclipse -d "{}" -n "{}" --bvd_id "{}"'
SPREADSHEET = 'https://docs.google.com/spreadsheets/d/1RsXZ5V7vMlouL6Yr5zM7wRPIMk2mV_Ql94B9q56C648/edit?usp=sharing'
SHEET_NAME = 'oprtun'
SAMPLE_SPREADSHEET_ID_input = SPREADSHEET.split('/')[5]
SAMPLE_RANGE_NAME = '{}!A1:AA1000'.format(SHEET_NAME)


# FUNCTIONS
def get_sheet():
    """
    this script collects data from the spread sheet

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


def write_data(sheet, vals):
    """
    writes data to cells in matrix format
    range='{}!H1:AA1000'.format(SHEET_NAME)
    writing only to row H1
    :param vals: list of one size lists
    :param sheet: the spreadsheet var
    :return: None
    """
    values = vals
    body = {
        'values': values
    }
    sheet.values().update(
                        spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
                        range='{}!H2:AA1000'.format(SHEET_NAME),  # H is the col of the run_id
                        valueInputOption='RAW',
                        body=body).execute()


def run_eclips_from_cmd(commands):
    """
    this func runs a list of commands in cmd eclips shell
    :param commands: list of strings which are eclips commands
    :return: dag id (string)
    """
    return cli.eclipse(commands[0], commands[1], commands[2])


def string_wrapper(st):
    """
    this func helps fixing some string incorrectness
    :param st: the string
    :return: fixed string if necessary
    """
    if st.endswith('\n'):
        return st.replace('\n', '')
    return st


def main():
    sheet = get_sheet()
    data = get_data(sheet)[1:]
    dags = []
    for row in data:
        # print(SYNTAX.format(string_wrapper(row[1]), string_wrapper(row[0]), string_wrapper(row[2])))
        dag_id = run_eclips_from_cmd([string_wrapper(row[1]), string_wrapper(row[0]), string_wrapper(row[2])])
        dags.append([dag_id])
    write_data(sheet, dags)


if __name__ == "__main__":
    main()
