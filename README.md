# README
Simple project to test usage of Crypto Compare's free API for fetching all historic
data of a specific crypto.

The Docker infra setup will launch a container with Jupyter Labs and PySpark to interface the code and data.

## Requirements
- Docker Engine
- Docker Compose

## Setup
```bash
git clone git@github.com:joaoferrao/cryptodata.git 
cd cryptodata
docker-compose up  # Access through the URL shown in stdout 
```

## Usage

### Download data
The first cells of `restructure_raw.ipynb` make usage of the main.py, but if you want to run a new one by default:
```python
from main import download_data
CRYPTOS = ['EOS', 'ETH']
download_data(CRYPTOS, currency='USD')
```

After successful running, `data_raw.csv` will be deposited in `crypto_data/data/data_raw.csv`.


## Acknowledgements
[Crypto compare FREE API](https://min-api.cryptocompare.com/) for providing a simple and free for all API RESTful API.
