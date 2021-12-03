# Header-Trailer-File-Adder

Automation implementation 
Generic Script to Add Header and Trailer to File

**Do NOT use this directly into client network. Check for any client confidentiality before reusing the code.**
The following (pseudo) Python script is used to add header and trailer tags to output files that are processed and sent down stream to the client.
This script begins by finding the count of each table as file_count which is used to help populate the Header and Trailer tags. After populating these variables with concatenated values, the script goes on to create Pandas Dataframes housing necessary column details, such as row ID’s, parameterized column names, project types, and time stamps. 
It then amalgamates a variety of the created parameters to populate the final file name variable. After this, formatting occurs to prepare the final_trailer_filename variable by setting the table names to uppercase. After a bit more formatting using the comma separation function, the output_file_path and final_trailer_filename are fed into a dbutils tag that will be used to write the trailer. This leads to the final set of commands which orchestrate the execution of the file, inserting necessary items to fulfil the header and trailer tag dependencies.  

=======================================================================================
# Python Pseudo Code 

# parameterized values
dbutils.widgets.text("provisioned_schema_name","", "provisioned_schema_name") 
dbutils.widgets.text("output_file_path","", "output_file_path") 
dbutils.widgets.text("business_date","", "business_date")
dbutils.widgets.text("output_ctrl_path","", "output_ctrl_path") 
dbutils.widgets.text("table_name","", "table_name")
dbutils.widgets.text("sub_header","", "sub_header")
dbutils.widgets.text("env", "", "env")
dbutils.widgets.text("freq", "", "freq")
dbutils.widgets.text("filename","","filename")
dbutils.widgets.text("col_names","", "col_names")   #an array of column names
dbutils.widgets.text("subfolder", "", "subfolder")
dbutils.widgets.text("domain","", "domain")

countdf = SELECT file count FROM {table_name} WHERE date =  {business_date}
  
file_count = first count in countdf
file_count_trailer= file_count+3

loop_cnt = 1
cum_sum = 0

while loop_cnt < file_count:
  cum_sum = cum_sum + loop count
  increments loop_cnt  

# formats & populates variable for trailer tag with counts obtained above
Trailer = f"Trailer~}}|{file_count_trailer}~}}|{cum_sum}"

date = spark.sql("select trimmed timestamp as cur_date")
cur_date = first cur_date from date

# formats & populates variable for Header tag using parameterized values and variables created above
header = f"File_HDR~}}|{env}~}}|UBS~}}|{freq}~}}|{cur_date}~}}|{business_date}~}}|{filename}~}}|"
sub_header = f'{sub_header}'

-	- ------------------------------------------- - -------

df = SELECT  'DTL', monotonically_increasing_id +1, {col_names},'BDR_USER', Timestamp() FROM provisioned.{table_name} WHERE date =  {business_date}
dfpd = Set df to Pandas()


-	- -------------------------------------- -  -------

# formats & populates file name variable with declared & parameterized values
final_file_name = f"{subfolder}_{domain}_{env}_{filename}_{freq}_{business_date}.txt"

upper_table_name = {table_name} to upper case
final_trailer_filename = "{upper_table_name} as .txt file"

# sets dfpd as string and applies formatting for the dataframe
myCsv = dfpd.astype(str).apply(lambda x: '~}|'.join(x), axis=1)

#sets dataframe to comma separated format amalgamating required variables and tags
myCsv.to_csv("{output_file_path}{final_file_name}", header =F,  Index=F,  quoting = csv.QUOTE_NONE, quotechar ="",  escapechar = "\\")

#writes concatenated file tags & parameters to output file
dbutils.fs.put("file:{output_file_path}{final_trailer_filename}", "contents", overwrite condition)

-	- ------------------------------ - 

#orchastrates execution of above code

f = open(f"{output_file_path}{final_file_name}", 'r')
s = f.read()
f.close()
l = s.splitlines()
l.insert(0, header)
l.insert(1, sub_header)
s = '\n'.join(l)
f = open(f"{output_file_path}{final_file_name}", 'w')
f.write(s)
f.write('\n'+Trailer)
f.close()

=======================================================================================


![image](https://user-images.githubusercontent.com/95493783/144638238-420351c3-8a0d-4686-87c1-82f5c063e9da.png)
