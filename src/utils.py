from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame 
# for file handling
import os
import json
from pyspark import SparkContext


class SparkUtilities:
    """
    This class contains the basic utilities to be used in the main
    """
    
    def __init__(self, config_path: str):
        """
        PARAMETERS
        ---------------
            config_path: str
                absolute_path of the JSON config file
        """
        self.config = self.load_config_file(config_path=config_path)
        spark_config = self.config.get('spark_config', {})

        self.spark = (SparkSession
                      .builder
                      .appName(spark_config.get('app_name', "default_app_name"))
                      .master(spark_config.get("master", "local"))
                      .getOrCreate()
                      )

        # change the system properties
        for property_name, value in spark_config.get('system_properties', {}).items() :
            if value :
                # if value is an empty string, leave it
                SparkContext.setSystemProperty(property_name,  value)

        self.spark.sparkContext.setLogLevel(spark_config.get("loglevel", "ALL"))

        print("Spark Session started.")


    def get_config(self):
        """
        return the config dict
        RETURNS:
        ---------------
            config: dict
        """
        return self.config
    

    def get_spark_session(self):
        """
        return the spark session created
        RETURNS:
        ---------------
            spark: SparkSession
        """
        return self.spark


    def read_df_from_file(self, filepath: str, **kwargs):
        """
        Read the provided file in a pyspark dataframe
        PARAMETERS
        ---------------
            filepath: str
                absolute path of the file to be read
            kwargs:
                inferSchema: bool
                    whether to deduce schema from the file. DEFAULT: True
                header: bool
                    whether the file contains header, only for csv files. DEFAULT: True
                delimiter: str
                    delimiter used in case of csv files. DEFAULT: comma (,)
                do_caching: bool
                    if dataframe needs to be cached. DEFAULT: False
                get_distinct: bool
                    if distinct operation needs to be done. DEFAULT: False
        RETURNS
        ---------------
            df: pyspark.sql.dataframe.DataFrame
                dataframe read from the file
        """

        ext = os.path.splitext(filepath)[1][1:]  # get the file extension without the '.'
        inferSchema = kwargs.get("inferSchema", True)
        header = kwargs.get("header", True)
        delimiter = kwargs.get("delimiter", ',')
        do_caching = kwargs.get("do_caching", False)
        get_distinct = kwargs.get("get_distinct", False)

        df = (self.spark.read.format(ext)
                .option('inferSchema', inferSchema)
                .option('header', header)
                .option('delimiter', delimiter)
                .load(filepath)
                )
        
        if get_distinct:
            df = df.distinct()

        if do_caching:
            df.cache()

        return df


    @staticmethod
    def write_file_from_dfs(df_list: list, location: str, **kwargs):
        """
        Writes the analysis output to a single txt file
        PARAMETERS
        ---------------
            df_list: list
                list containing dicts of format
                {"analysis_id": "id",  # int
                "analysis_text": "text for the analysis",  # str
                "analysis_output": "output of the analysis"}  # can be any value
            location: str
                absolute path of the file 
        RETURNS
        --------------
            None
        """

        data = ""
        # loop over all the analysis dicts
        for df_dict in df_list :
            # since the data will be written here, all actions will be performed here
            internal_string = f"\n{df_dict['analysis_id']}\n" + "*"*10 + "\n"
            internal_string += df_dict['analysis_text'] + "\n\nRESULT:\n\n"

            output = df_dict['analysis_output']
            if type(output) is DataFrame :
                # if the output is a dataframe, it needs to be converted into a text friendly string
                internal_string += output.toPandas().to_string(index=False)
            else :
                internal_string += str(output)

            print(internal_string + "\n" + "-" * 20 + "\n")
            
            data += internal_string + "\n" + "-" * 20 + "\n"

        # if the directory doesn't exist, make it
        directory_name = os.path.dirname(location)
        if not os.path.exists(directory_name):
            os.makedirs(directory_name)

        with open(location, "w") as f:
            f.write(data)

        print("\nOutput loaded to file ", location)

    
    @staticmethod
    def load_config_file(config_path: str) :
        """
        Loads the config file
        PARAMETERS
        --------------
            config_path: str
                absolute path of the JSON config file
        RETURNS
        --------------
            dict
        """
        config = json.load(open(config_path, "r")).get("config", {})

        if not config :
            raise Exception("No config provided. Exiting")
        
        return config