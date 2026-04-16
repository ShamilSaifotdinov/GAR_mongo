#!/usr/bin/env bash

py -m venv .venv
source .venv/Scripts/activate

pip install -r requirements.txt

py GAR_to_Mongo_new.py "..\\gar_xml (20250328)\\" 61 20250328
