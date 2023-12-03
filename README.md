# Magic the Gathering Analysis

This repository contains `python` code for accessing and manipulating Magic the Gathering datasets.

## Data Sources

### 17 Lands

The main source of data is [17 Lands](https://www.17lands.com/). 

#### Design

The data can be downloaded manually [here from 17 Lands](https://www.17lands.com/public_datasets). They are all in a compressed `.csv` format, typically `.csv.gz`, though some files are in `tar` format.

#### `game_data`

Directly reading these into memory as `pandas` dataframes is almost impossible due to the enormous column size. Nonetheless, the data are conveniently set up as a design matrix for statistical analysis. In particular, the dataset is a concatenation `[M | X]`, where `M` contains metadata, and is about 20 columns wide, and `X` is a sparse integer matrix containing card data counts in various positions (deck, hand, tutored, etc.) The design choice for reading this data into memory is to read chunks, extract the metadata into a typical `pandas` dataframe, and put the card data into a `scipy.sparse` matrix.

Most statistical operations are column oriented, so it is preferable to work with a [Concatenated Sparse Column Matrix (CSC)](https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csc_matrix.html). However, `.csv` data is inherently row oriented. Thus, the data are read in chunks of rows. The metadata are set aside as is, and the card data are read into a [Concatenated Sparse Row Matrix (CSR)](https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csr_matrix.html). After all chunks are read into memory, we can efficiently perform a [vertical concatenation](https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.vstack.html) and do a one-time conversion [from `CSR` to `CSC`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csr_matrix.tocsc.html).

The two pieces of data are then stored to disk for later processing with the metadata stored as a simple `.csv` and the card data as a `pickle` object.