#!/usr/bin/env bash

conda env create --prefix ./.env -f environment.yml
source activate base && conda activate ./.env
python GAR_to_Mongo_new.py "..\\gar_xml (20250328)\\" 61 20250328
