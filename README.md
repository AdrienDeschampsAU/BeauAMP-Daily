# BeauAMP-Daily

BeauAMP-Daily v1.0.0
-------------------------------------------------------------------------
*Base Étendue, Améliorée et Unifiée des Annonces des Marchés Publics - Daily*

* Copyright 2025 Adrien DESCHAMPS

BeauAMP-Daily is a free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation. For source availability and license information see `licence.txt`

* **GitHub repo:** https://github.com/AdrienDeschampsAU/BeauAMP-Daily
* **Data:** https://zenodo.org/records/17187786
* **Contact:** Adrien Deschamps <adrien.deschamps@univ-avignon.fr>
 
-------------------------------------------------------------------------

# Description
These scripts create the BeauAMP-daily database v.1.0.0 from contract and award notices published on the BOAMP (Bulletin Officiel des Annonces des Marchés Publics) under the Eforms format for public contracts that exceed European advertising threshold from January 1 2024. BeauAMP-Daily enhances this data by processing textual information into tabulars and adding individual information on contracting authorities and awarded firms through the estimation of SIREN identifiers and geolocation.

The produced database is directly available on [Zenodo](https://zenodo.org/records/17187786).

This work has been financed by CAP Territoires (Centrale d'achat public des territoires).

# Organization
This repository is composed of the following elements:
* The file "download.py" downloads original notices with the BOAMP API.
* The file "processing.py" processed the content of downloaded notices.
* The file "missing_sirens.py" lists the agents that need to be identified with their SIREN.
* The file "sirenisation.py" uses Google Search API to estimate the SIREN of unknown agents.
* The file "siren_import.py" integrates estimated identifiers into the processed data.
* The file "national_geolocation.py" geolocates agents located in France with the BAN (Base Adresse Nationale) API.
* The file "world_geolocation.py" geolocates agents located abroad with the Nominatim API.
* The folder "required_files" contains CSV files used for the consolidation of the dataset. The script also requires the latest SIRENE "Fichier StockUniteLegale" under Parquet format, which can be downloaded [here](https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret). This file is too heavy to be uploaded on the GitHub.

# Installation
You first need to install `python` and the following packages:
* `requests`
* `pandas`
* `numpy`
* `googleapiclient`


# Dependencies
Tested with Python version 3.12.3, with the following packages:
* `requests`: 2.32.2
* `pandas`: 2.3.1 
* `numpy`: 1.26.4
* `googleapiclient`: 2.176.0


# Data
The produced database is available online on ---, under three different forms:
