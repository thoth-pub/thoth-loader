[![Build Status](https://travis-ci.org/openbookpublishers/thoth-loader.svg?branch=master)](https://travis-ci.org/openbookpublishers/thoth-loader) [![Release](https://img.shields.io/github/release/openbookpublishers/thoth-loader.svg?colorB=58839b)](https://github.com/openbookpublishers/thoth-loader/releases) [![License](https://img.shields.io/github/license/openbookpublishers/thoth-loader.svg?colorB=ff0000)](https://github.com/openbookpublishers/thoth-loader/blob/master/LICENSE)

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
