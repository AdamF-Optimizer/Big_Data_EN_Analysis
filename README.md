# Big_Data_EN_Analysis

The Scala code is written as a proof-of-concept Spark application that processes web archive data (WARC files) to compare occurrences of words that have different spellings in UK English and US English. The proof of concept specifically focuses on the words "color"/"colour" and "center"/"centre".

The code extracts text from HTML content in the WARC files, counts how often these words appear, and associates these counts with the top-level domain of the source URL. It then aggregates and prints the total and per-domain occurrences for both American and British word sets for comparison.

To run the code, you must first source WARC files yourself, and you must install Apache Zeppelin to run the Zeppelin Notebook. For more details on installing Apache Zeppelin, see https://zeppelin.apache.org/.
