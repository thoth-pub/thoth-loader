[![Release](https://img.shields.io/github/release/thoth-pub/thoth-loader.svg?colorB=58839b)](https://github.com/thoth-pub/thoth-loader/releases) [![License](https://img.shields.io/github/license/thoth-pub/thoth-loader.svg?colorB=ff0000)](https://github.com/thoth-pub/thoth-loader/blob/master/LICENSE)

# Thoth Loader
Read metadata from a CSV or MARC XML file and insert it into Thoth

## Config
Install dependencies:
```
pip install -r requirements.txt
```

## CLI Usage

Available modes, depending on publisher input: `OBP` (Open Book Publishers), `punctum` (punctum books), `AM` (African Minds), `UWP` (University of Westminster Press), `WHP` (The White Horse Press)

### Live Thoth API
```
./loader.py --file ./data/metadata.csv --mode ${mode} --email ${email} --password ${password}
```

### Local Thoth API
```
./loader.py --file ./data/metadata.csv --mode ${mode} --email ${email} --password ${password} --client-url http://localhost:8080/graphql
```

## Docker Usage
### Live Thoth API
```
docker run --rm \
    -v /path/to/local/metadata.csv:/usr/src/app/metadata.csv \
    openbookpublishers/thoth-loader \
    ./loader.py \
        --file /usr/src/app/metadata.csv \
        --mode ${mode} \
        --email ${email} \
        --password ${password}
```

### Local Thoth API
```
docker run --rm \
    --network="host" \
    --volume /tmp/metadata.csv:/usr/src/app/metadata.csv \
    openbookpublishers/thoth-loader \
    ./loader.py \
        --file /usr/src/app/metadata.csv \
        --mode ${mode} \
        --email ${email} \
        --password ${password} \
        --client-url http://127.0.0.1:8000
```
