"""Downloads UCI frame list PDF and uploads to Google Sheets."""
import datetime
import hashlib
import os
import time
from io import BytesIO
from urllib.request import urlopen, Request
import numpy as np
import pandas as pd
import tabula  # 2.1.1, requires Java
try:
    import PyPDF4  # 1.27.0
except Exception:
    pass
# whether to write out to Google Sheets
write_gspread = True
if write_gspread:
    import gspread
"""
make PyPDF4 optional
test gspread to latest authentication
make home directory more flexible
make gspread optional
"""

try:
    os.chdir("/home/colin/UCI_pdf_reader")
    try:
        os.chdir("C:/Users/Owner/Documents/Personal/Projects/UCI-Framelist")
    except Exception:
        raise ValueError
except Exception:
    pass

frame_url = 'https://www.uci.org/docs/default-source/equipment/liste-des-modeles-de-cadres-et-fourches-homologues---list-of-approved-models-of-frames-and-forks.pdf'

usr_agent = 'Mozilla/5.0'
try:
    hdr = {'User-Agent': usr_agent}
    remoteFile = urlopen(Request(frame_url, headers=hdr)).read()
    memoryFile = BytesIO(remoteFile)

    pdfFile = PyPDF4.PdfFileReader(memoryFile)
    output = PyPDF4.PdfFileWriter()

    # remove non-table pages, hash first page to use as update reference
    for page in range(pdfFile.numPages):
        pageObj = pdfFile.getPage(page)
        pageText = (pageObj.extractText())
        if ("Frame code" in pageText) or ("Nom fourche" in pageText):
            print(page)
        output.addPage(pageObj)
        if page == 1:
            try:
                firstPageHash = hashlib.sha256(pageText.encode('utf-8')).hexdigest()
            except Exception:
                firstPageHash = np.nan

    stagingFilename = "cleaned-output.pdf"
    outputStream = open(stagingFilename, "wb")
    output.write(outputStream)
    outputStream.close()

    df_list = tabula.read_pdf(stagingFilename, multiple_tables=True, pages='all')

except Exception as e:
    repr(e)
    df_list = tabula.read_pdf(frame_url, multiple_tables=True, pages='all',
	                      user_agent=usr_agent)  # ,multiple_tables=True,pages='all' , flavor = 'stream'

df2 = pd.concat(df_list).reset_index(drop=True)
df2 = df2.replace(["", " ", "-", "/"], np.nan)
df2.loc[0,~pd.isnull(df2.iloc[0,:])]

for x in range(len(df2.index)):
    # if data in column 8+, shift over to the left
    try:
        null_test = len(df2.iloc[x, 7:]) - sum(pd.isnull(df2.iloc[x, 7:]))
    except Exception:
        null_test = 0
    if null_test > 0 :
        objects = df2.loc[x,~pd.isnull(df2.iloc[x,:])]
        if len(objects) == 6:
            df2.iloc[x, 1:(len(objects)+1)] = objects.values
        else:
            df2.iloc[x, 0:len(objects)] = objects.values
try:
    # df2.drop(columns=7, inplace = True)
    df2 = df2.iloc[:, 0:7]
except Exception:
    pass

df2.iloc[:, 0] = df2.iloc[:, 0].str.replace(r'[^\w\s.]', "").str.upper().str.strip()
df2.iloc[:, 0].replace(['NA','',' ', 'NaN','NULL'], np.nan, inplace = True)

df2 = df2.loc[~((df2.iloc[:,0]).isin(["FRAME NAME", "NOM CADRE", "DISC."])), :]

colnames = ["Frame name", "Fork name", "Disc.", "Sizes", "date", "Frame code", "Fork code"]
df2.replace(colnames, np.nan, inplace=True)
df2.columns = colnames
df2.dropna(how='all', inplace=True)

filler = np.where(pd.isnull(df2["Frame code"]), df2["Disc."], df2["Frame code"])
filler = np.where(df2['Fork name'].str.lower().isin(['rd', 'tt', "cx", 'tr']), filler, df2["Fork name"])
df2["Frame name"] = np.where(pd.isnull(df2["Frame name"]), filler, df2["Frame name"])

# Find dates even if in different columns
df2['Datetime'] = pd.to_datetime(df2['date'], format = '%d.%m.%Y', errors = 'coerce')
df2['Datetime'] = np.where(pd.isnull(df2['Datetime']), pd.to_datetime(df2['Sizes'], format = '%d.%m.%Y', errors = 'coerce'), df2['Datetime'])
df2['Datetime'] = np.where(pd.isnull(df2['Datetime']), pd.to_datetime(df2['Disc.'], format = '%d.%m.%Y', errors = 'coerce'), df2['Datetime'])
df2['Datetime'] = np.where(pd.isnull(df2['Datetime']), pd.to_datetime(df2['Frame code'], format = '%d.%m.%Y', errors = 'coerce'), df2['Datetime'])

df2.iloc[:,0] = df2.iloc[:,0].fillna(method='ffill') # fill down all frame names

# group datetime separately from others
temp = df2[["Frame name", 'Datetime']].groupby(colnames[0]).max()
temp2 = df2.select_dtypes('object').groupby(colnames[0]).max()
result = temp.merge(temp2, left_index=True, right_index=True)
result = result.reset_index(drop=False)
result.sort_values(by='Datetime', inplace=True, ascending=False)
result['RefreshTimeUTC'] = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

mostRecentValue = max(result['Datetime'])
if mostRecentValue > datetime.datetime.utcnow():
    mostRecentValue = datetime.datetime.utcnow()

result['Datetime'] = result['Datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
result = result.fillna(" ")
result.reset_index(drop=True, inplace=True)

# write to Google Sheets
if write_gspread:
    # Check whether an update to the spreadsheet is needed or not
    try:
        with open("lastUpdateTime.txt", "r") as text_file:
            existingValue = text_file.readlines()[0]
        if mostRecentValue > datetime.datetime.strptime(existingValue, "%Y-%m-%d"):
            update = True
        else:
            try:
                with open("firstPageHash.txt", "r") as text_file2:
                    existingHash = text_file2.readlines()[0]
                if existingHash != firstPageHash:
                    update = True
            except Exception:
                pass
        update = False
    except Exception:
        update = True

    with open("lastUpdateTime.txt", "w") as text_file:
        text_file.write("{}".format(mostRecentValue.strftime("%Y-%m-%d")))
    with open("firstPageHash.txt", "w") as text_file2:
        text_file2.write("{}".format(firstPageHash))

    try:
        gc = gspread.service_account(filename='UCI_List_secret.json')
    except Exception as e:
        repr(e)
        from oauth2client.service_account import ServiceAccountCredentials
        scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('UCI_List_secret.json', scope)
        gc = gspread.authorize(creds)

    sheet = gc.open('UCI Framelist').sheet1
    if len(sheet.col_values(1)) < 5:
        update = True

    if update:
        sheet.resize(1)
        sheet.resize(len(result.index) + 1)
        for index, row in result.iterrows():
            index_line = (int(index) + 2)
            print(index_line)
            sheet.insert_row(row.tolist(), index_line)
            time.sleep(4) # to prevent reaching the API resource limit
    else:
        print("No update deemed necessary")
else:
    result.to_csv("uci_framelist.csv")



# OLD/EXPERIMENTAL STUFF FOR REF

"""
# df = pd.DataFrame.from_dict(map(dict,df_list))

import camelot
tables = camelot.read_pdf(stagingFilename, pages = 'all', flavor = 'stream')  # flavor='stream' default Lattic  # pages = 'all'
df3 = pd.DataFrame()
for table in tables:
    df3 = df3.append(table.df)
df3.reset_index(drop = True, inplace = True)
df3 = df3.replace(["", " "], np.nan)
df3[~pd.isnull(df3.iloc[:,0])]
df3.loc[0,~pd.isnull(df3.iloc[0,:])]


import PyPDF4
pdfFileObj = open(pdf_directory, 'rb')
pdfReader = PyPDF4.PdfFileReader(pdfFileObj)

# import tempfile
# temp = tempfile.NamedTemporaryFile(suffix = '.pdf')
# pdf_location = temp.name.replace("\\", "/")
# temp.close()
# output.write(temp)
# temp.flush()
# cleanedMemoryFile = BytesIO()

df = tabula.read_pdf(stagingFilename,multiple_tables=True)

pageObj = pdfReader.getPage(43)
print(pageObj.extractText())

output.addPage(pdfReader.getPage(6))
output.addPage(file2.getPage(specificPageIndex))



pdf_content_1 = requests.get(frame_url).content
pdf_content_2 = requests.get('http://bar/foo.pdf').content
# Write to in-memory file-like buffers       
 
pdf_buffer_1 = BytesIO().write(pdf_content_1)
pdf_buffer_2 = StringIO.StringIO().write(pdf_content_2)
pdf_merged_buffer = StringIO.StringIO()
merger = PdfFileMerger()
merger.append(PdfFileReader(pdf_buffer_1))
merger.append(PdfFileReader(pdf_buffer_2))
pdf_merged_buffer = StringIO.StringIO()
merger.write(pdf_merged_buffer)



writer = PdfFileWriter()

outputStream = open("output.pdf","wb")
writer.write(outputStream)
outputStream.close()


DATA = b"hello bob"

def temp_opener(name, flag, mode=0o777):
    return os.open(name, flag | os.O_TEMPORARY, mode)

with tempfile.NamedTemporaryFile() as f:
    f.write(DATA)
    f.flush()
    with open(f.name, "rb", opener=temp_opener) as f:
        assert f.read() == DATA
   
import tempfile

print("Creating one named temporary file...")

temp = tempfile.NamedTemporaryFile()

try:  
    print("Created file is:", temp)
    print("Name of the file is:", temp.name)
finally:  
    print("Closing the temp file")
    temp.close()     

"""
