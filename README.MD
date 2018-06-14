### A project to import and export html files from mongodb database
##### To run the code: 
-specify html_folder and output_html_folder as the input and output folder of the html files in TransferMongo.py and unblock assertion_check() in TransferMongo.py if assertion testing is required, run TransferMongo.py

-folder test_html_files contains some testing html files for importing, test_html_files/output contains the example exported files

-module ConvertHtmlJson can also output a single json file for mannualy import from mongo shell (e.g. ConvertHtmlJson().convert_html2json(html_folder="/Users/chenjialu/Desktop/Rassure/code/test_html_files", json_path="test.json"))

##### Note: Single large files are stored in GridFS 