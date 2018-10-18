The weather data is analysed for particular year till Oct 15th 2018 data for a particular station (one file per station per year)
The ISD-Lite data contain a fixed-width formatted subset of the complete Integrated Surface Data (ISD) for a select number of observational elements. The data are typically stored in a single file corresponding to the ISD data.


This weather data project contains 6 folders.

1. data - which has the source data files
2. config - there are 2 files (one for batch config which contains the dates for running the script and 
                               second is the job config which contains the information required for running hive scripts that includes 
				location of script, schema details and table details)
3. log - it has 2 sub folders (one for the job logs which gets the job log information and other one is timestamp log information set by the 				script which updates the batch config file for dates)
4. hive - this folder contains the hive script planned for execution
5. py - this folder contains the python script required to run the hive script and update the config files and generating logs.
6. output_Data_tables - The tables that were created in hive is downloaded in file format to view (folder 1 - unpartitioned data (which has 			the source file without cleansing), folder 2 - weather_Data (with cleansing for specific fields), folder 3 - 			partitioned data (based on every run (updated_on timetamp) information)

Main things to be considered before running the script

1. Make sure the file is present in the data folder
2. Make sure the hive script location, HDFS log folder location and the target schema exists and updated in the config/qa/job-name.cfg folder
3. Make sure the log folders (both job log and timstamp log) is updated in the config/qa/batch.cfg folder

Once these steps are done, then we should be good to execute the script as rest of them are parameterized.

python /rhome/userid/project_weather/py/project_weather.py /rhome/userid/project_weather ./config/qa/batch.cfg ./config/qa/job-name.cfg 0 0
