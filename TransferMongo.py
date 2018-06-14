from pymongo import MongoClient
from pymongo.errors import DocumentTooLarge
from ConvertHtmlJson import ConvertHtmlJson
from gridfs import  GridFS
import bs4, requests
import urllib
import os
import gridfs

class TransferMongo(object):

    def __init__(self, host, port):
        """
        A class to import and export html files from mongodb
        :param host: the host of mongodb connection
        :param port: the port of mongdb connection
        """
        self.client = MongoClient(host, port)

    def import_htmlfiles(self, html_folder, dbname, colname):
        """
        A function to import html files to a mongo database collection
        :param dbname: database name
        :param colname: collection name
        :param html_folder: the folder stores the html files
        """
        json_docs, folder_files = ConvertHtmlJson().convert_html2json(html_folder=html_folder)
        db = self.client[dbname]
        collection = db[colname]
        collection_child = db[colname+"_child"]


        # GridFS collection for large file
        fs = GridFS(db)
        # store single documents
        for jdoc in json_docs:
            # get file name to check whether the document exist
            fn = jdoc["file_name"]
            # if it doesn't exist
            if collection.find({'file_name': fn}).count() == 0:
                try:
                    collection.insert(jdoc)
                    print("Inserted document: "+fn)
                except DocumentTooLarge:
                    if fs.find({"filename":fn}).count() == 0:
                        html_content = jdoc["html_content"]
                        html_content = str(html_content).encode("UTF-8")
                        fs.put(html_content, filename = fn)
                        print("Inserted document: " + fn)
                    else:
                        print(fn+" already exist!")
            else:
                print(fn+" already exist!")
        # store folder documents
        for fn, sheets in folder_files.items():
            for sheet in sheets:
                sn = sheet["sheet_name"]
                if collection_child.find({'file_name': fn, 'sheet_name':sn}).count() == 0:
                    collection_child.insert(sheet)
                    print("Inserted document: " + fn+'/'+sn)
                else:
                    print(fn+'/'+sn+ " already exist!")

    def fetch_htmlfiles(self, dbname, colname, output_html_folder):
        """
        A function to export html files from mongo database collection
        :param dbname: database name
        :param colname: collection name
        :param output_html_folder: the folder to output the html files
        :return htmls: a list of inserted html string
        :return fns: a list of inserted filenames
        """
        db = self.client[dbname]
        collection = db[colname]
        collection_child = db[colname + "_child"]
        htmls = []
        fns = []
        for doc in collection.find({}):
            # extract html string from mongodb
            file_name = doc['file_name']
            file_path = output_html_folder+'/'+file_name

            # for single file:
            if doc['html_type'] == "single":
                html = doc['html_content']
                fns.append(file_name)
                htmls.append(html)
                with open(file_path, 'w') as f:
                    f.write(html)

            # for multi files:
            if doc['html_type'] == "multi":
                # test whether the folder exist, if not create folder:
                if os.path.isdir(file_path)==False:
                    os.makedirs(file_path)

                for sheet in collection_child.find({"file_name": file_name}):
                    sn = sheet["sheet_name"]
                    sheet_path = output_html_folder+'/'+file_name+'/'+sn
                    html = sheet["html_content"]
                    with open(sheet_path, 'w') as f:
                        f.write(html)

        return htmls, fns

    def fetch_GridFS(self, dbname, output_html_folder):
        db = self.client[dbname]
        # GridFS collection for large file
        fs = GridFS(db)

        for fn in fs.list():
            file_path = output_html_folder+'/'+fn
            for fs_doc in fs.find({"filename":fn}):
                fs_doc  = fs_doc.read()
                with open(file_path, 'w') as f:
                    f.write(fs_doc.decode('utf-8'))





html_folder = "/Users/chenjialu/Desktop/Rassure/nltk/Html-Migrate-Mongo/test_html_files"
output_html_folder = html_folder + '/output'

tm = TransferMongo('localhost', 27017)
tm.import_htmlfiles(html_folder=html_folder, dbname="rassure_nltk", colname="fare_html")
htmls, fns = tm.fetch_htmlfiles(output_html_folder=output_html_folder, dbname="rassure_nltk", colname="fare_html")

tm.fetch_GridFS(output_html_folder=output_html_folder, dbname="rassure_nltk")



# check whether the string extract the html files match with the string extract from mongodb
"""def assertion_check():
    for i in range(len(fns)):
        file = urllib.request.urlopen('file://' + html_folder + "/" + fns[i])
        soup = bs4.BeautifulSoup(file, "html5lib")
        inserted_html = str(soup)
        assert inserted_html == str(htmls[i]), ("The string extracted from an inserted html file doesn't match with the html string reverted from mongodb")"""
# do an assertion_check
# Note: this will slow down the code!
# assertion_check()














