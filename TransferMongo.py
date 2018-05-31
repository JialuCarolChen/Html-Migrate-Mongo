from pymongo import MongoClient
from ConvertHtmlJson import ConvertHtmlJson
import bs4, requests
import urllib

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
        json_docs = ConvertHtmlJson().convert_html2json(html_folder=html_folder)
        db = self.client[dbname]
        collection = db[colname]
        for jdoc in json_docs:
            # get file name to check whether the document exist
            fn = jdoc["file_name"]
            # if it doesn't exist
            if collection.find({'file_name': fn}).count() == 0:
                collection.insert(jdoc)
                print("Inserted document: "+fn)
            else:
                print(fn+" already exist!")


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
        htmls = []
        fns = []
        for doc in collection.find({}):
            # extract html string from mongodb
            file_name = doc['file_name']
            file_path = output_html_folder+'/'+file_name
            html = doc['html_content']
            fns.append(file_name)
            htmls.append(html)
            with open(file_path, 'w') as f:
                f.write(html)
        return htmls, fns



html_folder = "/Users/chenjialu/Desktop/Rassure/code/test_html_files"
output_html_folder = html_folder + '/output'

tm = TransferMongo('localhost', 27017)
tm.import_htmlfiles(html_folder=html_folder, dbname="rassure_nltk", colname="fare_html")
htmls, fns = tm.fetch_htmlfiles(output_html_folder=output_html_folder, dbname="rassure_nltk", colname="fare_html")

# check whether the string extract the html files match with the string extract from mongodb
def assertion_check():
    for i in range(len(fns)):
        file = urllib.request.urlopen('file://' + html_folder + "/" + fns[i])
        soup = bs4.BeautifulSoup(file, "html5lib")
        inserted_html = str(soup)
        assert inserted_html == str(htmls[i]), ("The string extracted from an inserted html file doesn't match with the html string reverted from mongodb")
# do an assertion_check
# Note: this will slow down the code!
assertion_check()








