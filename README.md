[![Release](https://img.shields.io/github/release/thoth-pub/thoth-loader.svg?colorB=58839b)](https://github.com/thoth-pub/thoth-loader/releases) [![License](https://img.shields.io/github/license/thoth-pub/thoth-loader.svg?colorB=ff0000)](https://github.com/thoth-pub/thoth-loader/blob/master/LICENSE)

# Thoth Loader
Read metadata from a CSV and insert it into Thoth

## Config
Install dependencies:
```
pip install -r requirements.txt
```

## Usage

Available modes, depending on publisher input: `OBP` (Open Book Publishers), `punctum` (punctum books), `AM` (African Minds)

### Live Thoth API
```
./loader.py --file ./data/metadata.csv --mode ${mode} --email ${email} --password ${password}
```

### Local Thoth API
```
./loader.py --file ./data/metadata.csv --mode ${mode} --email ${email} --password ${password} --client-url http://localhost:8080/graphql
```

