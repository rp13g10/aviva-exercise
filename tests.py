"""
Basic set of tests for the PetitionLoader class.
"""
import unittest

from petition_loader import PetitionLoader


class TestLoader(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Create & execute methods (without overwriting saved data)
        loader = PetitionLoader("./data/")
        loader.load()
        loader.process()
        self.loader = loader

    def test_counts(self):
        """Check that the record counts for the first & second tasks match"""

        records_one = self.loader.first_output.count()
        records_two = self.loader.second_output.count()

        self.assertEqual(records_one, records_two, "Record counts don't match!")

    def test_keys(self):
        """Check that all petition IDs match up between first & second tasks"""

        records_one = self.loader.first_output.count()
        matching = (
            self.loader.first_output.select("petition_id")
            .join(
                self.loader.second_output.select("petition_id"),
                on="petition_id",
                how="inner",
            )
            .count()
        )

        self.assertEqual(records_one, matching, "Not all primary keys match!")

    def test_pkey(self):
        """Check that there are no empty values in the petition ID column"""

        records_all = self.loader.first_output.count()
        records_pop = self.loader.first_output.dropna(subset=["petition_id"]).count()

        self.assertEqual(
            records_all, records_pop, "Empty values in the primary key column!"
        )

    def test_schema_one(self):
        """Check that the schema for Task 1 matches the initial brief"""

        cols = set(self.loader.first_output.columns)
        tgt_cols = {"petition_id", "label_length", "abstract_length", "num_signatures"}

        self.assertEqual(cols, tgt_cols, "Output 1 columns don't match the brief!")

    def test_schema_two(self):
        """Check that the schema for Task 2 matches the initial brief"""

        cols = set(self.loader.second_output.columns)

        self.assertIn("petition_id", cols, "No primary key in Output 2!")

        self.assertEqual(len(cols), 21, "Not enough columns in Output 2!")


if __name__ == "__main__":
    unittest.main()
