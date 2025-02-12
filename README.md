<div style="text-align: center;">

[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Edgxtech/prise/blob/master/LICENSE)
</div>

# Blockchain Data Engineering

This is a companion repository for the 
[**"Getting Started as a Data Engineer with a Blockchain Scenario"**](https://medium.com/search?q=Geting+Started+as+a+Data+Engineer)
tutorial series.

## Setup

Configure a blockchain source which is a [*cardano-node*](https://github.com/IntersectMBO/cardano-node) 
with the [*ogmios*](https://github.com/CardanoSolutions/ogmios) bridge interface.

Options are to use:
1. *Ogmios* service from [Demeter.run](https://demeter.run) (perhaps only for quickstart and testing), or
2. Custom service

Configure **src/cardano_streamer/app.properties**
```properties
cardano.ogmios.address=mainnet-v6.ogmios-m1.demeter.run
cardano.ogmios.port=443
cardano.ogmios.ssl.enabled=True
cardano.ogmios.demeter.apikey=<your api key>

## The Cardano Node Ogmios Service - Own Cardano-Node / Ogmios Example
#cardano.ogmios.address=<your ogmios url or ip address>
#cardano.ogmios.port=<your ogmios port>
#cardano.ogmios.ssl.enabled=False

## Blockchain point to start streaming from
# Hint: slot corresponds to an absolute unix time[s] via; <slot> + 1591566291
cardano.start.point.block=4eaff920aa8392f4796365b97c6fc77d92eb9756940651e23b4ec28513eae8a4
cardano.start.point.slot=133303289
```

Configure environment
```bash
pip install -r src/cardano_streamer/requirements.txt
pip install -r src/etl_blockchain/requirements.txt
```

## Run

Start the streamer app
```bash
cd src/cardano_streamer
python main.py
```

Start the ETL app
*Duration of the run, is hard coded, change within*
```bash
cd src/etl_blockchain
python main.py
```

## Run the other example

Edit: `src/etl_vol_transfers/app.properties`

Make sure the streamer app is running (see above)

```bash
pip install -r src/etl_vol_transfers/requirements.txt
cd src/etl_vol_transfers
python main.py
```

## Run the Jupyter Notebook

```bash
pip install -r src/jupyter/requirements.txt
cd src/jupyter
# Install if needed; conda install jupyterlab
jupyter lab
```

## Python Version

Tested with Python 3.12

## Support
This project is made possible by Delegators to the [AUSST](https://ausstaker.com.au) Cardano Stakepool and 
supporters of [Edgx](https://edgx.tech) R&D