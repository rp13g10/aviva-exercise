import pyspark

from pyspark.sql.session import SparkSession

sc = SparkSession.builder.appName('aviva').getOrCreate()

sdf = sc.read.json('./data/input_data.json')

# TODO - Check 1 - Ensure input schema matches expectation

# No complex structures, unpack to tabular form
sdf = sdf.select(
    sdf['abstract._value'].alias('abstract'),
    sdf['label._value'].alias('label'),
    'numberOfSignatures'
)


"""
NOTE - Task Brief
Use pyspark to transform the data into the desired outputs
Include at least one test
Keep best practices in mind
Place code into a bitbucket/github repo

Output 1
Create a CSV file with one row per petition
Output schema
    petition_id, needs to be created
    label_length, number of words in the label field
    abstract_length, number of words in the abstract field
    num_signatures, number of signatures

Output 2
Create a CSV file with one row per petition
Output schema
    petition_id, as above
    1 column for each of the top 20 most common words
        top 20 based on all  petitions
        5 or more letters only

To Do
Prototype development, minimal viable solution
Wrap up into a basic ETL pipeline & implement DQ checks
Set up quick dummy dataset & use to implement tests
Build out readme with execution instructions
"""