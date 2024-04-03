
## Analysis on vehicle accidents across US

  

### Description

This project uses pyspark to analyse vehicle accidents across US for a brief amount of time

  

### Table of Contents

- [Installation](#installation)

- [Usage](#usage)

- [Config](#config)

- [Output](#output)


  

### Installation

Run the following pip command to install required packages

```bash

$ pip3  install  -r  requirements.txt
```

  

### Usage

The program uses PySpark to perform analysis. To run the program, execute
```bash
$ spark-submit src/main.py
```

### CONFIG
The code uses JSON config. Config format is as follow:

```JSON
{
    "config": {
        "spark_config": {
            "app_name": "name of the app",
            "master": "local/yarn",
            "system_properties": {
                "Property Name": "value",
                "spark.executor.memory": "1g"
            },
            "loglevel": "log level. can be ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN"
        },
        "input_file_paths" : {
            "charges_use": "path for charges_use file",
            "damages_use": "path for damages_use file",
            "endorse_use": "path for endorse_use file",
            "primary_person_use": "path for primary_person_use file",
            "restrict_use": "path for restrict_use file",
            "units_use_distinct": "path for units_use file"
        },
        "output_filepath": "output file path",
        "analysis": [
            "list of all the analysis text question. They will be printed in the file along with answers."
        ]
    }
}
```

### OUTPUT
Output is written to a txt file in the specified directory.
