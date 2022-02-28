import os
from functools import reduce
from glob import glob
from typing import List, Union


import pyspark
import sparknlp
import pyspark.sql.functions as ssf
import pyspark.sql.types as sst

from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame


# sdf = sc.read.json('./data/input_data.json')

# TODO - Check 1 - Ensure input schema matches expectation
# TODO - Check 2, ensure partition IDs match in both tables
# TODO - When saving, use input to allow creation
#        of non-existent directories

# No complex structures, unpack to tabular form
# sdf = sdf.select(
#     sdf['abstract._value'].alias('abstract'),
#     sdf['label._value'].alias('label'),
#     'numberOfSignatures'
# )


"""
NOTE - Task Brief
Use pyspark to transform the data into the desired outputs
Include at least one test
Keep best practices in mind
Place code into a bitbucket/github repo





To Do
Prototype development, minimal viable solution
Wrap up into a basic ETL pipeline & implement DQ checks
Set up quick dummy dataset & use to implement tests
Build out readme with execution instructions
"""

class PetitionLoader:

    def __init__(self, path_or_dir: Union[str, List[str]]) -> None:
        
        # Create spark context
        self.sc = SparkSession.builder.appName('aviva').getOrCreate()

        # Define expected input schema
        self.input_schema = self._get_schema()

        # Declare placeholders for loaded data
        self.petitions_in: DataFrame
        self.first_output: DataFrame
        self.second_output: DataFrame

        # Standardize user input
        self.paths = self._get_data_dirs(path_or_dir)


    def _get_data_dirs(self, path_or_dir: Union[str, List[str]]) -> list:
        '''Determine whether user input is a path or a directory.
        If a directory is provided, find all JSON files within it.'''

        if os.path.isfile(path_or_dir):
            # Single file, wrap in a list for consistency of output
            paths = [path_or_dir]
        elif os.path.isdir(path_or_dir):
            # Find all JSON files in the provided directory
            paths = list(glob(f"{path_or_dir}/*.json"))
        else:
            raise ValueError(f"Unable to locate input directory:\n\t{path_or_dir}")
        return paths

    def _read_file(self, file_dir: str) -> DataFrame:
        sdf = self.sc.read.json(file_dir, schema=self.input_schema)
        return sdf

    def _get_schema(self) -> sst.StructType:
        '''Check that the contents of the most recently
        read file match the expected schema.'''

        # Define the expected structure of the input file
        fields = [
            sst.StructField(
                'abstract',
                sst.StructType([
                    sst.StructField(
                        '_value',
                        sst.StringType(),
                        False
                    )
                ]),
                False
            ),
            sst.StructField(
                'label',
                sst.StructType([
                    sst.StructField(
                        '_value',
                        sst.StringType(),
                        False
                    )
                ]),
                False
            ),
            sst.StructField('numberOfSignatures', sst.IntegerType(), False)
        ]

        # Convert list to a spark schema object
        schema = sst.StructType(fields)

        return schema

    def _flatten_data(self, sdf: DataFrame) -> DataFrame:
        '''Unpack input spark dataframe into standard tabular format, by default
        text must be retrieved using the _value accessor.'''

        sdf = sdf.select(
            sdf['abstract._value'].alias('abstract'),
            sdf['label._value'].alias('label'),
            'numberOfSignatures'
        )

        return sdf

    def _generate_primary_key(self, sdf: DataFrame) -> DataFrame:
        '''Generate a synthetic primary key for each record. The SHA256
        algorithm is used in place of a random number to ensure any
        generated keys are consistent between & during runs.'''

        # Take text content of each record as a base for the primary key
        sdf = sdf.withColumn(
            'primaryKey',
            ssf.concat(
                ssf.lower(sdf['abstract']),
                ssf.lower(sdf['label'])
            ),
        )

        # Run SHA256 hashing algorithm on the base text to generate a primary key
        sdf = sdf.withColumn(
            'primaryKey',
            ssf.sha2(sdf['primaryKey'], 256),
        )

        return sdf

    def _remove_duplicates(self, sdf: DataFrame) -> DataFrame:
        '''Ensure dataset is distinct at one row per primary key.
        Drop any true duplicates, combine any records with the same
        primary key but differing signature counts.
        
        This approach would generally be adapted based on knowledge of
        the source system.'''



    def _generate_first_output(self, sdf):
        '''Output 1
        Create a CSV file with one row per petition
        Output schema
            petition_id, needs to be created
            label_length, number of words in the label field
            abstract_length, number of words in the abstract field
            num_signatures, number of signatures.'''
        
        sdf = sdf.select(
            sdf['primaryKey'].alias('petition_id'),
            ssf.size(ssf.split(sdf['label'], ' ')).alias('label_length'),
            ssf.size(ssf.split(sdf['abstract'], ' ')).alias('abstract_length'),
            sdf['numberOfSignatures'].alias('num_signatures')
        )

        self.first_output = sdf


    def _generate_second_output(self, sdf):
        '''Output 2
        Create a CSV file with one row per petition
        Output schema
            petition_id, as above
            1 column for each of the top 20 most common words
                top 20 based on all  petitions
                5 or more letters only'''
        pass

    def load(self) -> None:
        '''Perform all required steps to load in JSON file contents as a
        single spark dataframe'''

        # Read in all files in target directory
        input_sdfs = []
        for dir in self.paths:
            input_sdf = self._read_file(dir)
            input_sdfs.append(input_sdf)
        
        # Combine everything into a single spark dataframe
        input_sdf = reduce(lambda x, y: x.union(y), input_sdfs)

        # Store as class attribute
        self.petitions_in = input_sdf

    def process(self):
        '''Take the newly loaded petitions dataframe and execute the defined
        ETL process on it.'''

        # Tidy up the input schema
        petitions_out = self._flatten_data(self.petitions_in)

        # Define primary keys
        petitions_out = self._generate_primary_key(petitions_out)

        # Remove Duplicates

        # Generate first output
        self._generate_first_output(petitions_out)


if __name__ == '__main__':

    loader = PetitionLoader('./data/')

    # Trigger file load, no ETL beyond initial read operation
    loader.load()

    # Trigger file processing
    loader.process()