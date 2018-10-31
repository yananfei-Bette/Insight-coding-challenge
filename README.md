# Insight-coding-challenge

This is a Insight Take-home challenge

# Environment Requirement

I used Intelij to build my code. Scala 2.11.12 and Spark 2.3.1 are required.

# Way to run the code

You can find my source Scala code in src file. A simple way to run my code is use the .jar file. The .jar file will take the input .csv which is in the input folder, and generate top_10_occupations.txt and top_10_states.txt automatically. You can find results in the output folder.

Here is a command line templet.

'''
$SPARK_HOME/bin/spark-submit --class Yanan_Fei Yanan_Fei.jar
'''

Because of repo size limitation. I put my .jar file on google drive. You can find it by https://drive.google.com/file/d/1HBdic3mQuGc7or0EZSbr6vigdCzbfRGX/view?usp=sharing

# Repo directory structure

The directory structure of my repo is:
'''
      ├── README.md 
      ├── src
      │   └──Yanan_Fei.Scala
      ├── input
      │   └──h1b_input.csv
      ├── output
      |   └── top_10_occupations.txt
      |   └── top_10_states.txt
      ├── Yanan_Fei.jar
'''

# Run another input.csv using .jar

Make sure put .csv into the input folder and change filename into h1b_input.csv

# Some useful info

Make sure in the input .csv, the name of application status column contains key word "status", occupation name contains "SOC_name", and state contains "work" and "state".