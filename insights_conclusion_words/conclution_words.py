##########################################
# this script purpose is to convert table lines into eclips syntax
# Date - 15/2/2022
# Writer - Chen Scheim
# To - Dana Gindes the queen

############## INSTRUCTIONS  #############
# 1. copy the downloaded file from DataBricks to this folder
# 2. name the file export.csv
# 3. if the export.csv cols are not: signal_id, float_value, body
#    there will be problem with indexing
# 5. run


from funcs.funcs import get_sheet, get_data
import pandas


SPREADSHEET = 'https://docs.google.com/spreadsheets/d/1vOPeELYnWxEhTNDMKZL3_zWxDjelQlqJG-R4BpbXPa0/edit?usp=sharing'
SHEET_NAME = 'flashpoint'
CSV = 'export.csv'


def terms(data):
    """
    list of terms from the vr_signals excel
    :param data: list of lists from spreadsheet
    :return: dict of the signal and the value is the list of terms
    """
    res = {}
    for row in data:
        if row[4] == 'Signal' and (row[6] or row[7]):
            l = row[6].split(', ') + row[7].split(', ')
            if '' in l:
                l.remove('')
            res[row[3]] = l
    return res


def read_csv():
    """
    reading csv
    :return: list of lists
    """
    data = pandas.read_csv('export.csv').values.tolist()
    return data


def match_terms(data, the_terms):
    """
    going through the results from the exported file and match the signal ID of the result to the list of terms
    and see how many times each term bumped
    :param data: the list of lists from the result file
    :param the_terms: dictionary of words
    :return: None
    """
    for row in data:
        words = {}
        if row[0] in the_terms.keys():
            for word in the_terms[row[0]]:
                if word in row[2]:
                    if word in words.keys():
                        words[word] += 1
                    else:
                        words[word] = 1
        else:
            print('problem')
        print(row[0], row[1])
        print(words)

def main():
    sheet = get_sheet()
    data = get_data(sheet, SPREADSHEET, SHEET_NAME)[1:]  # list of lists
    the_terms = terms(data)
    output_data = read_csv()
    match_terms(output_data, the_terms)

if __name__ == "__main__":
    main()