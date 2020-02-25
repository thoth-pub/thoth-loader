# Thoth Loader
Read metadata from a CSV and insert it into Thoth

## Config
Copy `.env.example` into `.env` and update database credentials.

Install dependencies:
```
pip install -r requirements.txt
```

## Usage
```
./loader.py --file ./data/metadata.csv --mode OBP --client-url http://thoth/graphql
```
