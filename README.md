# README
Simple project to test usage of Crypto Compare's free API for fetching all historic
data of a specific crypto.

## Requirements
- Conda (or miniconda)

## Setup
```bash
git clone git@github.com:joaoferrao/cryptodata.git 
cd cryptodata
conda create -n crypto python=3.6
source activate crypto
pip install -r requirements.txt
```

## Usage
1. Run main.py and it will download the list of cryptos identified in the CRYPTOS list.
2. `docker-compose up` to launch a pyspark notebook.



## Acknowledgements
[Crypto compare FREE API](https://min-api.cryptocompare.com/) for providing a simple and free for all API RESTful API.
