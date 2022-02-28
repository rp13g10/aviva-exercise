# Aviva Exercises

This repo has been set up to satisfy the following requirements:

## Task 1
Create a CSV file with one row per petition, containing the following four columns: 
* petition_id: a unique identifier for each petition (this is not present in the 
input data and needs to be created) 
* label_length: the number of words in the label field 
* abstract_length: the number of words in the abstract field 
* num_signatures: the number of signatures

The CSV should be sorted by the number of signatures.

## Task 2
Create a CSV file with one row per petition, containing the following 21 columns: 
* petition_id: a unique identifier for each petition (this is not present in the 
input data and needs to be created) 
* One column for each of the 20 most common words across all petitions, only 
counting words of 5 or more letters, storing the count of each word for each 
petition.

For example, if “government” is one of the 20 most common (5+ letter) words, one column 
should be titled government. If the first petition includes the word “government” three 
times, and the second petition does not mention “government”, then the government
column should read 3 for the first petition and 0 for the second petition.

## Basic Usage

* Execute the script `python .\petition_loader.py`
* Run tests `python .\tests.py`