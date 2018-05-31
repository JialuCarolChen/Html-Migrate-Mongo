import bs4, requests
import urllib
import json
import os
import pymongo

class ConvertHtmlJson:

    def convert_a_html2json(self, file_name, file_path):
        """
        A function to convert a html file to a dictionary
        :param file_name: name of the file
        :param file_path: path of the file
        :return: a dictionary of the html file
        """
        # path of the html files
        file = urllib.request.urlopen('file://' + file_path)
        soup = bs4.BeautifulSoup(file, "html5lib")
        # initialize the dict to store a html
        html_dict = dict()
        html_dict['file_name'] = file_name
        # store html content
        html_dict['html_content'] = str(soup)
        return html_dict


    def convert_html2json(self, html_folder, json_path=None):
        """
        A function to convert a folder of html files to a json file for importing from mongo shell (if json_path is given),
        it also return a list of dictionaries, each dict represents a json document
        :param html_folder: the path of the folder stores the html files
        :param json_path: the path of the json file
        :return return a list of dicts (json documents)
        """
        json_file = []
        for file_name in os.listdir(html_folder):
            if file_name.endswith(".html"):
                file_path = html_folder + "/" + file_name
                print(file_path)
                json_dict = self.convert_a_html2json(file_name, file_path)
                json_file.append(json_dict)
        # if json path is given
        if json_path is not None:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_file, f, ensure_ascii=False)
        return json_file

# DEMO:
# ConvertHtmlJson().convert_html2json(html_folder="/Users/chenjialu/Desktop/Rassure/code/test_html_files", json_path="test.json")




