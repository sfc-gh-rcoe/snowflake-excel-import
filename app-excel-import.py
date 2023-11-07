from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StringType, TimestampType, FloatType, StructField, VariantType
import snowflake.snowpark.files as files
import snowflake.snowpark.functions as F
from dotenv import load_dotenv
from collections import OrderedDict
from datetime import datetime
import xmltodict
import pandas as pd
import os
import sys
import streamlit as st
from io import StringIO
import openpyxl



load_dotenv()
my_creds = {
	"account": os.environ["account_name"],
	"user": os.environ["account_user"],
	"password": os.environ["account_password"],
	"role": os.environ["account_role"],
	"schema": os.environ["account_schema"],
	"database": os.environ["account_database"],
	"warehouse": os.environ["account_warehouse"]
}
#
# Function from Stackoverflow for flattening an XML document
# 
# Attribution URL: https://stackoverflow.com/questions/38852822/how-to-flatten-xml-file-in-python
#
def flatten_dict(d):
	def items():
		for key, value in d.items():
			if isinstance(value, dict):
				for subkey, subvalue in flatten_dict(value).items():
					yield key + "." + subkey, subvalue
			else:
				yield key, value

	return OrderedDict(items())

    
def write_excel_to_table(input_file_name): 

	#
	# Before running the following line, a determination must be made for how to populate the 'my_creds' dictionary with authentication credentials
	# Could be a JSON bundle, could use the dotenv module, or retreive creds from Azure Key Vault
	#
    session = Session.builder.configs(my_creds).create()


    the_time = datetime.now()
    the_date_suffix = the_time.strftime("%m%d%Y_%H_%M_%S")
	# stage_name = "@XMLTEST"
	# input_file_name = "books-sample.xml"
    output_table_name = "EXCEL_FILE_IMPORT"
    output_table_name = output_table_name + "_" + the_date_suffix
    # file_url = "{}".format(input_file_name)
    # with open(file_url, 'r', encoding='utf-8') as f:
    #     excelcontent_read = f.read()


    # Create a pandas dataframe representing an initial flattened version of the XML input---more work to do
    # s = pd.read_excel(excelcontent_read)
    s = pd.read_excel(input_file_name, sheet_name=None)
	
    st.write(s)
	
    for sheet in s:
        my_df = session.createDataFrame(s[sheet.title()])
        t_output_table_name = output_table_name + "_" + str(sheet.title())
        my_df5 = my_df.write.mode("overwrite").save_as_table(table_name=t_output_table_name, table_type='transient')
    return t_output_table_name

st.title("Excel Importer")


m_session1 = Session.builder.configs(my_creds).create()

record_count = 0

uploaded_files = st.file_uploader(label = "Select the file(s) against which to match", accept_multiple_files=True, type = ['xls', 'xlsx'])
t_df = m_session1.create_dataframe(data=[[1,2]], schema=['a', 'b'])
if ((uploaded_files)):
    l_df = m_session1.create_dataframe(data=[[1,2]], schema=['a', 'b'])
    for t_file in uploaded_files:
        if t_file is not None:
            # amt_of_data = t_file.getvalue()
            t_name = write_excel_to_table(t_file)
            t_df = m_session1.table(t_name)
    st.dataframe(t_df.limit(25).toPandas())







