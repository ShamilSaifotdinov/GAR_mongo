#!/usr/bin/env bash

conda env create --prefix ./.env -f environment.yml
source activate base && conda activate ./.env
python GAR_to_Mongo_new.py "..\\..\\Source\\gar\\gar_xml (20260414)\\" 62 20260414
