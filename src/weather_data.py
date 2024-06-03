"""
File name: weather_data.py
Author: Mahdi Bazyar
Date: 06/02/2024

"""

import os
import sys
import socket
import pandas as pd, time
import psycopg2
import psycopg2.extras as extras 
from glob import glob


try:
    # import base_functions
    from base import base_functions

except Exception as e:
    # Handle exception while importing base_functions
    current_directory = os.path.split(os.path.realpath(__file__))[0]
    sys.path.append(current_directory)
    from base import base_functions


class weather_class(base_functions):
    def __init__(self, caller, config):
        """
        Initilalize the weather class. read in configuration and create a logger object.
        param caller: string. name of the child class inheriting from this class.
        param config: string. file path for configuraton file in json format.
        return: None
        
        """
        try:
            super(weather_class, self).__init__(caller, config)

        except Exception as e:
            print("Process failed at weather() class initialization. {0}".format(str(e)))


    def read_wx_data(self):
         try:
            # Folder path to provided wx_data
            folder_path = self.config['wx_data_folder_path']
            txt_files = os.listdir(folder_path)
            # Extract file names as station_is and store them in a list to be later added to dataframe station_id column
            station_id = []
            for file in txt_files:
                id = file.split('.')
                station_id.append(id[0])
  
            runner.logger.info("\nTotal number of txt files in folder {0} is: {1}".format(folder_path, (len(txt_files))))

            # Make dataframe from txt files
            files = glob(folder_path + '/*.txt')
            get_dataframe = lambda file: pd.read_csv(file, header=None, delimiter='\t', names=['date', 'max_temperature', 'min_temperature', 'precipitation'])

            # Make dictionty from dataframes of txt files
            dictionary_of_dataframe = {file: get_dataframe(file) for file in files} 

            # Convert dictioanry to iterable list .collect()[0][0].year
            list_dataframe = [x for x in dictionary_of_dataframe.values()]

            # Iterate over station_id list and list_dataframe to insert new columns into dataframe
            for id, df in zip(station_id, list_dataframe):
                dates_integer = df['date'].to_list()
                # Covert integer list to string list
                dates_string = map(str, dates_integer)
                # Extract year from date and store it in year list
                year = []
                for date in dates_string:
                    y = int(str(date)[:4])
                    year.append(y)   

                # Using DataFrame.insert() to add 'Year' column and insert year data
                df.insert(0, 'station_id', id, True)
                df.insert(2, 'year', year, True)

                # Insert data into postgres table 
                postgress_connection = runner.postgress_connection(df)


            runner.logger.info("Data are extracted from txt files and inserted into postgres database successfully")
     
         except Exception as e:
            runner.logger.error("Process failed at read_wx_data(). {0}".format(str(e)))  


    def postgress_connection(self, df):
        """
        Postgres database connection.
        param: df: dataframe 
        return: None
        
        """
        try:
            # Establish postgres database connection
            database_connection = psycopg2.connect(
                dbname = self.config["database_name"],
                user = self.config["postgres_username"],
                host = self.config["database_host"],
                password = self.config["postgres_password"])

            # Set auto commit True
            database_connection.autocommit = True

            if database_connection:
                runner.logger.info("Database connection established successfully")
            else:
                runner.logger.info("Database connection failed. Please verify coneciton parameters.")

            # Execute the SQL query to get the count of rows in the table
            if database_connection:
                with database_connection.cursor() as cursor:
                    sql = "SELECT COUNT(*) AS count FROM ctva.weather_data"
                    cursor.execute(sql)
                    count = cursor.fetchone()
                    runner.logger.info("Table records before insert data: {}".format(count[0]))
                    
                    # Convert dataframe to tuples
                    tuples = [tuple(x) for x in df.to_numpy()] 

                    # Retrieve dataframe columns 
                    column = ','.join(list(df.columns))

                    # SQL query to insert data into weather_data table
                    # Handle duplicate records
                    query = "INSERT INTO %s(%s) VALUES %%s" % ('ctva.weather_data', column)  

                    try:
                        # Execute the query 
                        extras.execute_values(cursor, query, tuples) 
                        database_connection.commit() 

                    except (Exception, psycopg2.DatabaseError) as e: 
                        runner.logger.error("Error while inserting data into postgres database: {0}".format(str(e))) 
                        # Make rollback call
                        database_connection.rollback() 
                        # close the cursor
                        cursor.close() 
                        
                runner.logger.info("The dataframe is inserted successfully") 

                with database_connection.cursor() as cursor:
                    # execute the SQL query to get the count of rows in the table
                    sql = "SELECT COUNT(*) AS count FROM ctva.weather_data"
                    cursor.execute(sql)
                    count = cursor.fetchone()
                    runner.logger.info("Table records after inserting dataframe data: {}".format(count[0]))
                    # close the cursor
                    cursor.close()

        except Exception as e:
            runner.logger.error("Process failed at postgress_connection(). {0}".format(str(e))) 

        finally:
            # Close database conneciton 
            database_connection.close()
            runner.logger.info("Database connection closed successfully")

    
    def data_analysis(self):
        """
        Query on weather_data table on Postgres database to calculate the below and store the result in weather_analysis table.

        For every year, for every weather station, calculate:
        * Average maximum temperature (in degrees Celsius)
        * Average minimum temperature (in degrees Celsius)
        * Total accumulated precipitation (in centimeters)

        param: None
        return: None 
        
        """
        try:
            # Establish postgres database connection
            database_connection = psycopg2.connect(
                dbname = self.config["database_name"],
                user = self.config["postgres_username"],
                host = self.config["database_host"],
                password = self.config["postgres_password"])

            # Set auto commit True
            database_connection.autocommit = True

            if database_connection:
                runner.logger.info("Database connection established successfully")
            else:
                runner.logger.info("Database connection failed. Please verify coneciton parameters.")
            
            # Define the SQL query
            # Convert temreatures from tenths of a degree Celsius to degree Celsius
            # Convert precipitation from tenths of a millimeter to centimeters
            # Ignore records with value -9999 as null value
            # Handle duplicate insert
            if database_connection:
                with database_connection.cursor() as cursor:  
                    sql_query = """
                        INSERT INTO ctva.weather_analysis (station_id, year, avg_max_temperature_celsius, avg_min_temperature_celsius, total_precipitation_cm)
                        SELECT
                            station_id,
                            year,
                            ROUND(AVG(NULLIF(max_temperature, -9999) / 10)::numeric, 2) AS avg_max_temperature_celsius,
                            ROUND(AVG(NULLIF(min_temperature, -9999) / 10)::numeric, 2) AS avg_min_temperature_celsius,
                            ROUND(SUM(NULLIF(precipitation, -9999) / 100)::numeric, 2) AS total_precipitation_cm
                        FROM
                            ctva.weather_data
                        GROUP BY
                            station_id,
                            year
                        ORDER BY
                            station_id,
                            year
                        ON CONFLICT (station_id, year)
                        DO UPDATE SET
                            avg_max_temperature_celsius = EXCLUDED.avg_max_temperature_celsius,
                            avg_min_temperature_celsius = EXCLUDED.avg_min_temperature_celsius,
                            total_precipitation_cm = EXCLUDED.total_precipitation_cm
                    """
                    
                    cursor.execute(sql_query)
                    
                    runner.logger.info("\nQuery result is inserted in ctva.weather_analysis successfully.")
                    
        except Exception as e:
            runner.logger.error("Process failed at data_analysis(). {0}".format(str(e))) 

        finally:
            # Close database conneciton 
            database_connection.close()
            runner.logger.info("Database connection closed successfully")

              
if __name__ == "__main__":
    """
        Main function to implement the requirements 
        
    """
    try:
        process_status = 'success'
        start_time = time.time()
        # Set current directory
        current_directory = os.path.split(os.path.realpath(__file__))[0]
        # Add configuration file
        config = os.path.join(current_directory, 'weather_data_config.json')
        # Initialize weather_class class
        runner = weather_class('WeatherDataProcess', config)

        runner.logger.info("Process started at {0}-{1}\n\n".format(runner.date, runner.time))
        runner.logger.info("\nMachine Host Name: {0}".format(socket.gethostname()))

        # read wx_data directory and convert .txt files into pandas dataframes
        wx_data = runner.read_wx_data()

        # Analyze weather data on postgress database
        analysis = runner.data_analysis()

        runner.logger.info("\nProcess successfully executed. Please check log for details at [{0}]".format(runner.log_directory))

    except Exception as e:
            process_status = 'fail'
            print("Process failed at main() function. {0}".format(str(e)))

    finally:
        process_run_time = round(((time.time() - start_time) / 60), 2)
        print("Process Run Time: {0} Minutes".format(process_run_time))
        runner.logger.info("\nProcess Run Time: {0} Minutes".format(process_run_time))

        if process_status == 'success':
            exit(0)
        else:
            exit(1)


