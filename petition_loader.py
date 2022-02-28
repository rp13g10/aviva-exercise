import os
from functools import reduce
from glob import glob
from typing import List, Union

import sparknlp
import pyspark.ml as sm
import pyspark.sql.functions as ssf
import pyspark.sql.types as sst

from pyspark.sql import DataFrame

from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import Tokenizer, LemmatizerModel, StopWordsCleaner


class PetitionLoader:
    def __init__(self, path_or_dir: Union[str, List[str]]) -> None:

        # Create spark context
        # self.sc = SparkSession.builder.appName('aviva').getOrCreate()
        self.sc = sparknlp.start(spark32=True)

        # Define expected input schema
        self.input_schema = self._get_schema()

        # Declare placeholders for loaded data
        self.petitions_in: DataFrame
        self.first_output: DataFrame
        self.second_output: DataFrame

        # Standardize user input
        self.paths = self._get_data_dirs(path_or_dir)

    def _get_data_dirs(self, path_or_dir: Union[str, List[str]]) -> list:
        """Determine whether user input is a path or a directory.
        If a directory is provided, find all JSON files within it."""

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
        """Check that the contents of the most recently
        read file match the expected schema."""

        # Define the expected structure of the input file
        fields = [
            sst.StructField(
                "abstract",
                sst.StructType([sst.StructField("_value", sst.StringType(), False)]),
                False,
            ),
            sst.StructField(
                "label",
                sst.StructType([sst.StructField("_value", sst.StringType(), False)]),
                False,
            ),
            sst.StructField("numberOfSignatures", sst.IntegerType(), False),
        ]

        # Convert list to a spark schema object
        schema = sst.StructType(fields)

        return schema

    def _flatten_data(self, sdf: DataFrame) -> DataFrame:
        """Unpack input spark dataframe into standard tabular format, by default
        text must be retrieved using the _value accessor."""

        sdf = sdf.select(
            sdf["abstract._value"].alias("abstract"),
            sdf["label._value"].alias("label"),
            "numberOfSignatures",
        )

        return sdf

    def _generate_primary_key(self, sdf: DataFrame) -> DataFrame:
        """Generate a synthetic primary key for each record. The SHA256
        algorithm is used in place of a random number to ensure any
        generated keys are consistent between & during runs."""

        # Take text content of each record as a base for the primary key
        sdf = sdf.withColumn(
            "primaryKey",
            ssf.concat(ssf.lower(sdf["abstract"]), ssf.lower(sdf["label"])),
        )

        # Run SHA256 hashing algorithm on the base text to generate a primary key
        sdf = sdf.withColumn(
            "primaryKey",
            ssf.sha2(sdf["primaryKey"], 256),
        )

        return sdf

    def _remove_duplicates(self, sdf: DataFrame) -> DataFrame:
        """Ensure dataset is distinct at one row per primary key.
        Drop any true duplicates, combine any records with the same
        primary key but differing signature counts.

        This approach would generally be adapted based on knowledge of
        the source system."""

        # Any true duplicates are removed (matching key & signature count)
        sdf = sdf.drop_duplicates(subset=["primaryKey", "numberOfSignatures"])

        # Partial matches are added together, perhaps the petitions are being
        # signed on multiple websites?
        sdf = sdf.groupby("primaryKey").agg(
            ssf.first(sdf["label"]).alias("label"),
            ssf.first(sdf["abstract"]).alias("abstract"),
            ssf.sum(sdf["numberOfSignatures"]).alias("numberOfSignatures"),
        )

        return sdf

    def _generate_first_output(self, sdf: DataFrame) -> None:
        """Task:
        Create a CSV file with one row per petition
        Output schema
            petition_id, needs to be created
            label_length, number of words in the label field
            abstract_length, number of words in the abstract field
            num_signatures, number of signatures."""

        sdf = sdf.select(
            sdf["primaryKey"].alias("petition_id"),
            ssf.size(ssf.split(sdf["label"], " ")).alias("label_length"),
            ssf.size(ssf.split(sdf["abstract"], " ")).alias("abstract_length"),
            sdf["numberOfSignatures"].alias("num_signatures"),
        )

        self.first_output = sdf

    def _get_tokens(self, sdf: DataFrame) -> DataFrame:
        """Take the raw text for each petition, perform some basic text processing
        and create a 'tokens_out' field, which contains an array of tokens for each
        record in the dataset."""

        assembler = DocumentAssembler().setInputCol("label").setOutputCol("document")

        tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("token")

        lemma = (
            LemmatizerModel.pretrained().setInputCols(["token"]).setOutputCol("lemma")
        )

        remover = (
            StopWordsCleaner.pretrained()
            .setInputCols("lemma")
            .setOutputCol("cleaned")
            .setCaseSensitive(False)
        )

        finisher = Finisher().setInputCols(["cleaned"]).setOutputCols("tokens_out")

        nlp_pipe = sm.Pipeline().setStages(
            [assembler, tokenizer, lemma, remover, finisher]
        )

        sdf = nlp_pipe.fit(sdf).transform(sdf)
        sdf = sdf.select("primaryKey", "tokens_out")

        return sdf

    def _explode_tokens(self, sdf: DataFrame, min_length: int = 5) -> DataFrame:

        # Explode out to one row per record, per token
        sdf = sdf.select("primaryKey", ssf.explode(sdf["tokens_out"]).alias("token"))

        # Restrict to the minimum word length
        sdf = sdf.filter(ssf.length(sdf["token"]) >= min_length)

        # Set to lower case
        sdf = sdf.withColumn("token", ssf.lower(sdf["token"]))

        return sdf

    def _get_top_n(self, sdf: DataFrame, n_words: int = 20) -> DataFrame:
        """Take the processed tokens for each record, calculate the top 20
        most common words in the dataset."""

        # Calculate occurrences of each word in the dataset
        counts = sdf.groupby("token").agg(
            ssf.count(sdf["primaryKey"]).alias("occurrences")
        )

        # Sort in descending order, restrict to top n_words records
        counts = counts.orderBy(counts["occurrences"].desc()).limit(n_words)

        return counts

    def tokens_to_features(self, sdf: DataFrame) -> DataFrame:

        # Get count of each token per record
        sdf = sdf.groupby("primaryKey", "token").agg(ssf.count("token").alias("count"))

        # Pivot out to wide form, one row per record
        # one column per token
        sdf = sdf.groupby("primaryKey").pivot("token").sum("count")

        return sdf

    def _generate_second_output(self, sdf: DataFrame) -> DataFrame:
        """Task:
        Create a CSV file with one row per petition
        Output schema
            petition_id, as above
            1 column for each of the top 20 most common words
                top 20 based on all  petitions
                5 or more letters only"""

        # Get an array of tokens for each record
        tokens = self._get_tokens(sdf)

        # Explode out to one row per record, per token
        tokens = self._explode_tokens(tokens, min_length=5)

        # Get top 20 most common tokens
        top_n = self._get_top_n(tokens, n_words=20)
        top_n = top_n.drop("occurrences")

        # Restrict tokens to only the top 20 most common
        tokens = tokens.join(top_n, on="token", how="inner")

        # Convert to wide-form
        features = self.tokens_to_features(tokens)

        # Make sure all records are present in the output, even if they
        # don't contain any of the top 20 words
        all_keys = self.first_output.select("petition_id").withColumnRenamed(
            "petition_id", "primaryKey"
        )
        features = all_keys.join(features, on="primaryKey", how="left")

        # Fill in any missing values with 0
        to_fill = [x for x in features.columns if x != "primaryKey"]
        features = features.fillna(0, subset=to_fill)

        # Set schema as required
        features = features.withColumnRenamed("primaryKey", "petition_id")

        self.second_output = features

    def load(self) -> None:
        """Perform all required steps to load in JSON file contents as a
        single spark dataframe"""

        # Read in all files in target directory
        input_sdfs = []
        for dir in self.paths:
            input_sdf = self._read_file(dir)
            input_sdfs.append(input_sdf)

        # Combine everything into a single spark dataframe
        input_sdf = reduce(lambda x, y: x.union(y), input_sdfs)

        # Store as class attribute
        self.petitions_in = input_sdf

    def process(self) -> None:
        """Take the newly loaded petitions dataframe and execute the defined
        ETL process on it."""

        # Tidy up the input schema
        petitions_out = self._flatten_data(self.petitions_in)

        # Define primary keys
        petitions_out = self._generate_primary_key(petitions_out)

        # Remove Duplicates
        petitions_out = self._remove_duplicates(petitions_out)

        # Generate first output
        self._generate_first_output(petitions_out)

        # Generate second output
        self._generate_second_output(petitions_out)

    def save(self) -> None:
        """Take the two generated datasets and save them to CSV format"""

        # First requested output
        self.first_output.toPandas().to_csv(
            "./outputs/first_output.csv", index=False, encoding="utf8", header=True
        )

        # Second requested output
        self.second_output.toPandas().to_csv(
            "./outputs/second_output.csv", index=False, encoding="utf8", header=True
        )


if __name__ == "__main__":

    loader = PetitionLoader("./data/")

    # Trigger file load, no ETL beyond initial read operation
    loader.load()

    # Trigger file processing
    loader.process()

    # Save outputs
    loader.save()
