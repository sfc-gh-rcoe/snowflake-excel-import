from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StringType, TimestampType, FloatType, StructField, VariantType
import snowflake.snowpark.files as files
import snowflake.snowpark.functions as F
from dotenv import load_dotenv
from collections import OrderedDict
from datetime import datetime
import xmltodict
import pandas as pd
import polars as pl
import modin.pandas
import os
import streamlit as st
import re




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

t_output_table_name = {}
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

    # Retrieve from the environment the name we want to use for our output table prefix
    output_table_name = os.environ["excel_table_prefix"]
    output_table_name = output_table_name + "_" + the_date_suffix + "_SHEET"


    # Get the number of dictionary keys that would result in a call to read_excel() with sheet_name=None
    # n_sheets = len(pl.read_excel(input_file_name, sheet_name=None, sheet_id=0).keys())
    s = pl.read_excel(input_file_name, sheet_id=0)
    
    # Iterate through the sheets in the workbook, referencing them by index
    # We are using this method because a call to read_excel() with sheet_name=None results
    # in a problematic dictionary if any of the sheets have non-alphanumeric characters
    for i in s:
        # st.write(s["Sheet1"])
        # s = pl.read_excel(input_file_name, )
        i_name = "{}".format(i.title())
        temp_df = s[i]
        try:       
            # if len(s) > 1:
            #     my_df = session.createDataFrame(pl.DataFrame()._from_dict(temp_df))
            # else:
            my_df = session.createDataFrame(temp_df.to_pandas())
            t_index = output_table_name + "_{}".format(re.sub(' ', '_', i))
            t_output_table_name[t_index] = t_index
            my_df5 = my_df.write.mode("overwrite").save_as_table(table_name=t_output_table_name[t_index], table_type='transient')
            output_table_name = os.environ["excel_table_prefix"]
        except ValueError:
            st.write("Skipping the sheet named: '{}'.  Pandas read_excel() dictionary creation problem.".format(i_name))
            output_table_name = os.environ["excel_table_prefix"]

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
            t_arr = write_excel_to_table(t_file)
            for t_table in t_arr:
                try:
                    t_df = m_session1.table(t_table)
                    st.write(t_table)
                    st.dataframe(t_df.limit(25).toPandas())
                except:
                    st.write("The table you tried to open doesn't exist.")







