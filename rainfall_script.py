import pandas as pd
import requests as req
import os, glob
import tabula
import datetime
import boto3
from botocore.exceptions import ClientError
import logging

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
    os.chdir(r"pdf-downloads")
    os.getcwd()
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

    with_data
    #with_data.remove('output-3013002.csv')

    #Combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f) for f in with_data ])
    #Export to csv
    combined_csv.to_csv("combined_csv.csv", index=False, encoding='utf-8-sig')

#Rename File with Date
def rename_file():
    os.getcwd()
    current_date = datetime.datetime.today().strftime ('%d-%b-%Y')
    os.rename('combined_csv.csv', r'combined_csv_' + str(current_date) + '.csv')
    file_name = 'combined_csv_' + str(current_date) + '.csv'
    print("Combination complete")
    return file_name

def upload_to_aws(file_name, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, 'ssdu-rainfall', object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

#Run Script
def main():
    download_dataset()
    combine_dataset()
    file_name = rename_file()
    upload_to_aws(file_name, object_name=None)
    print("Upload Complete")
    os.remove(file_name)

if __name__ == "__main__":
    main()