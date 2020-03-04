import pandas as pd
import requests as req
import os, glob
import tabula
import datetime

url = 'http://jpsscada.selangor.gov.my/PDFGen?trendType=RF&Id='
#Station ID
id_num = open("pdf_urls.txt", "r")

#Download Datasets
def download_dataset():
    for x in id_num:
        name = x.strip('\n')
        print(name)
        link = url+name
        myfile = req.get(link, allow_redirects=True)
        open('pdf-downloads/output.pdf', 'wb').write(myfile.content)
        os.rename (r'pdf-downloads/output.pdf',r'pdf-downloads/output-'+str(name)+'.pdf')
        print("Download complete!")
    else:
        #Convert PDF to CSV
        tabula.convert_into_by_batch("pdf-downloads", output_format='csv')
        print("Converting complete!")
        id_num.close()

#Combine CSV
def combine_dataset():
    #set working directory
    os.chdir(r"\\path\pdf-downloads")
    #find all csv files in the folder
    #save result in list -> list_of_files
    extension = 'csv'
    list_of_files = [i for i in glob.glob('*.{}'.format(extension))]

    with_data = []
    no_data = []

    #Check empty DF
    for x in list_of_files:
        if os.stat(x).st_size > 0:
            with_data.append(x)
        else:
            no_data.append(x)

    #with_data.remove('output-3013002.csv')

    #Combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f) for f in with_data ])
    #Export to csv
    combined_csv.to_csv("combined_csv.csv", index=False, encoding='utf-8-sig')

#Rename File with Date
def rename_file():
    os.chdir(r"\\path\pdf-downloads")
    current_date = datetime.datetime.today().strftime ('%d-%b-%Y')
    os.rename('combined_csv.csv', r'combined_csv_' + str(current_date) + '.csv')
    print("Combination complete")

#Run Script
def main():
    download_dataset()
    combine_dataset()
    rename_file()

if __name__ == "__main__":
    main()

    