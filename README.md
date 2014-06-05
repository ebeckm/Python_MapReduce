
A Python MapReduce Tool
=======================

This package provides mapReduce functionality for Python users on a standard Mac.
MapReduce can speed data work on machines that have sufficient memory and multiple cores.
The tool is built for use with the [Acquire Valued Shoppers Dataset](https://www.kaggle.com/c/acquire-valued-shoppers-challenge)
and can easily be hacked for use in other applications.

How to Use
----------

This tool was built for use with the
[Acquire Valued Shoppers Dataset](https://www.kaggle.com/c/acquire-valued-shoppers-challenge)
hosted by Kaggle.

* Download that dataset and save it to the "data" directory.
* Run the main() function from the dataStoreSetup.py to populate your "chunks" directory.
* Use buildFeatures.py to generate some example features.

Algorithm Details
-----------------

Data is first distributed across a number of CSV files which are stored in a
directory "chunks"

Mappers are initialized using the Python multiprocessing library.  Each mapper reads
a pre-assigned subset of the CSV files stored in the "chunks" directory.
Results from the mapper processes are stored in another set of CSV files in
a directory titled "reduce_store".  A shared list of locks is used to prevent
mappers from simultaneously writing to the same reduce_store CSV file.

Reducers are called to consolidate the output from the mappers.
Reducers store their results in a CSV filed located in the "features" directory.


Dependencies
------------
* Pandas
* Numpy
* Scipy


Built and tested in Python 2.7.5

