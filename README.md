# Ranked Retrieval

Basic java program uses the cosine similarity score to rank documents for a given query.

## Description

This project is designed to use the cosine similarity score to identify the most relevant document for a query. It takes in a 
posting list as an input, generates accumulators, stores them in a [LevelDB](https://github.com/fusesource/leveldbjni), then
accepts a query from a user and uses the cosine similarity score to return the most relevant documents. If the second "reset" 
parameter is not specified then the program will use the existing [LevelDB](https://github.com/fusesource/leveldbjni) instead
of recalculating all of the accumulators.

## Course

This project was created for Big Data at 
[Westmont College](https://www.westmont.edu/computer-science) 
in the Fall of 2020.

