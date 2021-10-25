# `hibou`

`hibou` is a data pipeline that:
* extracts biological knowledge models and ontology from the [EMMAA](https://emmaa.indra.bio/home) platform;
* transforms them into a set of grounded objects compatible with the preprocessing pipelines of `bgraph`, `grafer`, etc.;
* loads the result onto a MinIO server (AWS S3).

## Installation

```bash
pyenv install 3.8.7
pyenv local 3.8.7
python3 -m venv .venv
source ./.venv/bin/activate
pip install -r requirements.txt
```

## Local Run

```bash
python3 prefect_hibou_pipe.py
```

## Remote Run
```bash
conda activate prefect
python3 prefect_hibou_pipe_remote.py
```

## Parameters

- `todo_model`: <str> Space-separated list of model EMMAA IDs to be processed (`'all'` for all models)
    - default = `'covid19'` 

- `exclude_model`: <str> Space-separated list of model EMMAA IDs to be excluded
    - default = `'food_security'`

- `emmaa_ontology`: <str> File name of the EMMAA ontology to be used in the pipeline
    - default = `'bio_ontology_v1.10_export_v1'`

- `emmaa_api_url`: <str> URL of the EMMAA API endpoints
    - default = `'https://emmaa.indra.bio'`

- `emmaa_s3_url`: <str> URL of the EMMAA S3 bucket
    - default = `'https://emmaa.s3.amazonaws.com'`

- `pipeline_name`: <str> Name of the pipeline, used in building the file paths on the storage S3 bucket
    - default = `'hibou'`

- `s3_url`: <str> URL of the storage S3 bucket, used in building the file paths on the storage S3 bucket
    - default = `'http://10.64.18.171:9000'`

- `s3_bucket`: <str> Name of the storage S3 bucket, used in building the file paths on the storage S3 bucket
    - default = `'aske'`

- `s3_data_path`: <str> Path of the directory for storing raw data, used in building the file paths on the storage S3 bucket
    - default = `'research/BIO/data'`

- `s3_dist_path`: <str> Path of the directory for storing distribution files, used in building the file paths on the storage S3 bucket
    - default = `'research/BIO/dist'`

- `s3_dist_version`: <str> Version of the distribution files, used in building the file paths on the storage S3 bucket
    - default = `'v4.0'`

- `namespaces_priority`: <str> Space-separated list of database namespaces, ordered by priority and used to sort the model node groundings
    - default = `'FPLX UPPRO HGNC UP CHEBI GO MESH MIRBASE DOID HP EFO'`

- `print_opt`: <bool> Whether to print out intermediate statuses
    - default = `False`
