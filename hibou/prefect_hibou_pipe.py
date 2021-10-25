# %%[markdown]
# Author: Nelson Liu 
#
# Email: [nliu@uncharted.software](mailto:nliu@uncharted.software)

# %%[markdown]
# Content: 
#
# 1. Import modules
#  
# 2. Define Prefect tasks
#  
# 2.1 Extract models and tests from EMMAA S3 bucket
#
# 2.2 Extract model/test statements from EMMAA S3 bucket and load statements to storage S3 bucket
#  
# 2.3 Transform
#  
# 3. Define Prefect flow
#  
# 4. Run the flow locally or register it on the Prefect server


# %%[markdown]
# # Import Modules

import datetime
import hibou_lib as hlib
import importlib
import prefect
from prefect import task, Flow, Parameter, unmapped

# %%[markdown]
# ```bash
# scp nliu@10.64.6.12:/home/nliu/projects/aske/pipelines/hibou/hibou_lib.py .
# scp nliu@10.64.6.12:/home/nliu/projects/aske/pipelines/hibou/prefect_hibou_pipe.py .
# scp nliu@10.64.6.12:/home/nliu/projects/aske/pipelines/hibou/prefect_hibou_pipe_remote.py .
#
# scp hibou_lib.py centos@10.64.18.191:/home/centos/flows/aske/hibou/
# scp prefect_hibou_pipe.py centos@10.64.18.191:/home/centos/flows/aske/hibou/
# scp prefect_hibou_pipe_remote.py centos@10.64.18.191:/home/centos/flows/aske/hibou/
#
# ssh centos@10.64.18.191
# cd /home/centos/flows/aske/hibou
# conda activate prefect
# python3 prefect_hibou_pipe_remote.py
# ```

# %%[markdown]
# # Define Prefect Flow

# %%
with Flow('hibou data pipeline') as flow:

    # Get logger
    logger = prefect.context.get('logger')
    

    # Define pipeline parameters
    # emmaa_models = <str> space-separated list of `id_emmaa` of the models or 'all' 
    todo_models = Parameter('todo_models', default = 'covid19')
    exclude_models = Parameter('exclude_models', default = 'food_insecurity')
    emmaa_ontology = Parameter('emmaa_ontology', default = 'bio_ontology_v1.10_export_v1')
    emmaa_api_url = Parameter('emmaa_api_url', default = 'https://emmaa.indra.bio')
    emmaa_s3_url = Parameter('emmaa_s3_url', default = 'https://emmaa.s3.amazonaws.com')

    pipeline_name = Parameter('pipeline_name', default = 'hibou')
    s3_url = Parameter('s3_url', default = 'http://10.64.18.171:9000')
    s3_bucket = Parameter('s3_bucket', default = 'aske')
    s3_data_path = Parameter('s3_data_path', default = 'research/BIO/data')
    s3_dist_path = Parameter('s3_dist_path', default = 'research/BIO/dist')
    s3_dist_version = Parameter('s3_dist_version', default = 'v4.0')

    namespaces_priority = Parameter('namespaces_priority', default = 'FPLX UPPRO HGNC UP CHEBI GO MESH MIRBASE DOID HP EFO')
    print_opt = Parameter('print_opt', default = False)


    # Generate main path of storage S3 bucket
    s3_path_storage = hlib.build_path(path = [s3_dist_path, '/', s3_dist_version, '/', pipeline_name, '/'], print_opt = print_opt)


    # Snapshot date
    snapshot_date = datetime.date.today().isoformat()


    # Extract and load EMMAA ontology
    # url = f"https://emmaa.s3.amazonaws.com/integration/ontology/bio_ontology_v1.10_export_v1.json.gz"
    logger.info(f'Extracting EMMAA ontology and loading to S3 bucket...')
    s3_path_emmaa_onto = hlib.build_path(
        path = ['integration/ontology/', emmaa_ontology, '.json.gz'], 
        print_opt = print_opt
    )
    ontology = hlib.extract_data(
        domain = emmaa_s3_url, 
        path = s3_path_emmaa_onto, 
        file_ext = '.json.gz'
    )
    # s3_path_storage_onto = hlib.build_path(
    #     path = [s3_data_path, '/ontologies/', emmaa_ontology, '.json'], 
    #     print_opt = print_opt
    # )
    # hlib.load_obj_to_s3(
    #     obj = ontology, 
    #     s3_url = s3_url, 
    #     s3_bucket = s3_bucket, 
    #     s3_path = s3_path_storage_onto
    # )


    # Extract and transform model and test metadata
    logger.info('Extracting model and test metadata from EMMAA S3 bucket...')
    (models, tests, paths) = hlib.extract_models_tests_paths_from_emmaa(emmaa_api_url = emmaa_api_url, print_opt = print_opt)


    # # Load model metadata to storage S3 bucket
    # logger.info('Loading model metadata to storage S3 bucket...')
    # s3_path_storage_models = hlib.build_path(
    #     path = [s3_dist_path, '/', s3_dist_version, '/', pipeline_name, '/', 'models.jsonl']
    # )
    # preamble = hlib.get_obj_preamble(obj_type = 'models')
    # hlib.load_obj_to_s3(
    #     obj = models, 
    #     s3_url = s3_url, 
    #     s3_bucket = s3_bucket, 
    #     s3_path = s3_path_storage_models, 
    #     preamble = preamble
    # )


    # # Load test metadata to storage S3 bucket
    # logger.info('Loading test metadata to storage S3 bucket...')
    # s3_path_storage_tests = hlib.build_path(
    #     path = [s3_dist_path, '/', s3_dist_version, '/', pipeline_name, '/', 'tests.jsonl'],
    #     print_opt = print_opt
    # )
    # preamble = hlib.get_obj_preamble(obj_type = 'tests')
    # hlib.load_obj_to_s3(
    #     obj = tests, 
    #     s3_url = s3_url, 
    #     s3_bucket = s3_bucket, 
    #     s3_path = s3_path_storage_tests, 
    #     preamble = preamble
    # )


    # Extract raw statements associated with each test (corpus)
    # url = f"https://emmaa.s3.amazonaws.com/tests/{test['id_emmaa']}.jsonl"
    logger.info('Extracting test statements from EMMAA S3 bucket...')
    s3_path_emmaa_test_stats = hlib.build_path_iter.map(
        path = unmapped('tests/'),
        path_ = unmapped('.jsonl'),
        path_iter = tests, 
        key_iter = unmapped('id_emmaa'),
        print_opt = unmapped(print_opt)
    )
    tests = hlib.extract_data.map(
        domain = unmapped(emmaa_s3_url), 
        path = s3_path_emmaa_test_stats, 
        file_ext = unmapped('.jsonl'),
        return_obj = tests, 
        return_obj_key = unmapped('statements')
    )


    # # Load test statements to storage S3 bucket
    # # url = f"http://10.64.18.171:9000/minio/aske/research/BIO/data/tests/{test['id_emmaa']}/{snapshot_date}/latest_statements.jsonl"
    # logger.info('Loading test statements to storage S3 bucket...')
    # s3_path_storage_test_stats = hlib.build_path_iter.map(
    #     path = unmapped('research/BIO/data/tests/'),
    #     path_ = unmapped(f"/{snapshot_date}/latest_statements.jsonl"),
    #     path_iter = tests, 
    #     key_iter = unmapped('id_emmaa'),
    #     print_opt = unmapped(print_opt)
    # )
    # hlib.load_obj_to_s3.map(
    #     obj = tests, 
    #     s3_url = unmapped(s3_url), 
    #     s3_bucket = unmapped(s3_bucket), 
    #     s3_path = s3_path_storage_test_stats, 
    #     preamble = unmapped(None),
    #     obj_key = unmapped('statements')
    # )


    # Read `todo_models` to know what model data to process
    todo_models_ = hlib.read_todo_models(todo_models = todo_models, exclude_models = exclude_models, models = models, print_opt = print_opt)
    models_ = hlib.filter_objs(
        objs = models, 
        obj_key = 'id_emmaa', 
        obj_vals = todo_models_
    )


    # Extract raw statements associated with each model
    # url = f"https://emmaa.s3.amazonaws.com/assembled/<id_emmaa>/latest_statements_<id_emmaa>.jsonl"
    logger.info('Extracting model statements from EMMAA S3 bucket...')
    s3_path_emmaa_model_stats = hlib.build_path_iter.map(
        path = unmapped('assembled/'),
        path_ = unmapped('/latest_statements_'), 
        path_iter = models_, 
        key_iter = unmapped('id_emmaa')
    )
    s3_path_emmaa_model_stats_ = hlib.build_path_iter.map(
        path = s3_path_emmaa_model_stats, 
        path_ = unmapped('.jsonl'),
        path_iter = models_, 
        key_iter = unmapped('id_emmaa'),
        print_opt = unmapped(print_opt)
    )
    models_ = hlib.extract_data.map(
        domain = unmapped(emmaa_s3_url), 
        path = s3_path_emmaa_model_stats_, 
        file_ext = unmapped('.jsonl'),
        return_obj = models_, 
        return_obj_key = unmapped('statements')
    )


    # # Load model statements to storage S3 bucket
    # # url = f"http://10.64.18.171:9000/minio/aske/research/BIO/data/models/{model['id_emmaa']}/{snapshot_date}/latest_statements.jsonl"
    # logger.info('Loading model statements to storage S3 bucket...')
    # s3_path_storage_model_stats = hlib.build_path_iter.map(
    #     path = unmapped('research/BIO/data/models/'),
    #     path_ = unmapped(f"/{snapshot_date}/latest_statements.jsonl"),
    #     path_iter = models_, 
    #     key_iter = unmapped('id_emmaa')
    # )
    # hlib.load_obj_to_s3.map(
    #     obj = models_, 
    #     s3_url = unmapped(s3_url), 
    #     s3_bucket = unmapped(s3_bucket), 
    #     s3_path = s3_path_storage_model_stats, 
    #     preamble = unmapped(None),
    #     obj_key = unmapped('statements')
    # )


    # Extract statement curation associated with each model
    # url = f"https://emmaa.indra.bio/curated_statements/model['id_emmaa']}"
    logger.info('Extracting statement curation associated with each ')
    s3_path_emmaa_model_curation = hlib.build_path_iter.map(
        path = unmapped('curated_statements/'),
        path_ = unmapped(''),
        path_iter = models_, 
        key_iter = unmapped('id_emmaa'),
        print_opt = unmapped(print_opt)
    )
    models_ = hlib.extract_data.map(
        domain = unmapped(emmaa_api_url), 
        path = s3_path_emmaa_model_curation, 
        file_ext = unmapped('.json'),
        return_obj = models_, 
        return_obj_key = unmapped('curation')
    )


    # # Load statement curation to storage S3 bucket
    # # url = f"http://10.64.18.171:9000/minio/aske/research/BIO/data/models/{model['id_emmaa']}/{snapshot_date}/curation.json"
    # logger.info('Loading statement curation to storage S3 bucket...')
    # s3_path_storage_model_curation = hlib.build_path_iter.map(
    #     path = unmapped('research/BIO/data/models/'),
    #     path_ = unmapped(f"/{snapshot_date}/curation.json"),
    #     path_iter = models_, 
    #     key_iter = unmapped('id_emmaa'),
    #     print_opt = unmapped(print_opt)
    # )
    # hlib.load_obj_to_s3.map(
    #     obj = models_, 
    #     s3_url = unmapped(s3_url), 
    #     s3_bucket = unmapped(s3_bucket), 
    #     s3_path = s3_path_storage_model_curation, 
    #     preamble = unmapped(None),
    #     obj_key = unmapped('curation')
    # )


    # Filter `paths` object by given `todo_models_` list
    paths_ = hlib.filter_objs(
        objs = paths,
        obj_key = 'id_emmaa_model',
        obj_vals = todo_models_
    )


    # Extract path data associated with each model-test pair
    # url = f"https://emmaa.s3.amazonaws.com/paths/model['id_emmaa']}/test['id_emmaa']_latest_paths.jsonl"
    logger.info('Extracting path data associated with each model-test pair')
    s3_path_emmaa_paths = hlib.build_path_iter.map(
        path = unmapped('paths/'),
        path_ = unmapped('/'),
        path_iter = paths_, 
        key_iter = unmapped('id_emmaa_model'),
        print_opt = unmapped(print_opt)
    )
    s3_path_emmaa_paths_ = hlib.build_path_iter.map(
        path = s3_path_emmaa_paths,
        path_ = unmapped('_latest_paths.jsonl'),
        path_iter = paths_,
        key_iter = unmapped('id_emmaa_test'),
        print_opt = unmapped(print_opt)
    )
    paths_ = hlib.extract_data.map(
        domain = unmapped(emmaa_s3_url), 
        path = s3_path_emmaa_paths_, 
        file_ext = unmapped('.jsonl'),
        return_obj = paths_, 
        return_obj_key = unmapped('emmaa_data')
    )


    # # Load path data associated with each model-test pair to storage S3 bucket
    # # url = f"http://10.64.18.171:9000/minio/aske/research/BIO/data/models/{model['id_emmaa']}/{snapshot_date}/{test['id_emmaa']}_latest_paths.jsonl"
    # logger.info('Loading path data to storage S3 bucket...')
    # s3_path_storage_paths = hlib.build_path_iter.map(
    #     path = unmapped('research/BIO/data/models/'),
    #     path_ = unmapped(f"/{snapshot_date}/"),
    #     path_iter = paths_, 
    #     key_iter = unmapped('id_emmaa_model'),
    #     print_opt = unmapped(print_opt)
    # )
    # s3_path_storage_paths_ = hlib.build_path_iter.map(
    #     path = s3_path_storage_paths,
    #     path_ = unmapped("_latest_paths.jsonl"),
    #     path_iter = paths_, 
    #     key_iter = unmapped('id_emmaa_test'),
    #     print_opt = unmapped(print_opt)
    # )
    # hlib.load_obj_to_s3.map(
    #     obj = paths_, 
    #     s3_url = unmapped(s3_url), 
    #     s3_bucket = unmapped(s3_bucket), 
    #     s3_path = s3_path_storage_paths_, 
    #     preamble = unmapped(None),
    #     obj_key = unmapped('emmaa_data')
    # )


    # Transform raw model statements into node, edge lists
    logger.info('Transform EMMAA statements into node, edge lists...')
    models_ = hlib.transform_statements_to_node_edge_lists.map(model = models_, print_opt = unmapped(print_opt))


    # # Load model evidences to storage S3 bucket
    # logger.info('Loading model evidences to storage S3 bucket...')
    # s3_path_storage_model_evidences = hlib.build_path_iter.map(
    #     path = unmapped(s3_path_storage),
    #     path_ = unmapped(f'/evidences.jsonl'),
    #     path_iter = models_, 
    #     key_iter = unmapped('id_emmaa'),
    #     print_opt = unmapped(print_opt)
    # )
    # preamble = hlib.get_obj_preamble(obj_type = 'evidences')
    # hlib.load_obj_to_s3.map(
    #     obj = models_, 
    #     s3_url = unmapped(s3_url), 
    #     s3_bucket = unmapped(s3_bucket), 
    #     s3_path = s3_path_storage_model_evidences, 
    #     preamble = unmapped(preamble),
    #     obj_key = unmapped('evidences')
    # )


    # # Load model docs to storage S3 bucket
    # logger.info('Loading model docs to storage S3 bucket...')
    # s3_path_storage_model_docs = hlib.build_path_iter.map(
    #     path = unmapped(s3_path_storage),
    #     path_ = unmapped(f'/docs.jsonl'),
    #     path_iter = models_, 
    #     key_iter = unmapped('id_emmaa'),
    #     print_opt = unmapped(print_opt)
    # )
    # preamble = hlib.get_obj_preamble(obj_type = 'docs')
    # hlib.load_obj_to_s3.map(
    #     obj = models_, 
    #     s3_url = unmapped(s3_url), 
    #     s3_bucket = unmapped(s3_bucket), 
    #     s3_path = s3_path_storage_model_docs, 
    #     preamble = unmapped(preamble),
    #     obj_key = unmapped('docs')
    # )


    # # Transform model edges with curation status
    # models_ = hlib.curate_edge_list.map(model = models_)


    # # Transform EMMAA path data to Hibou format
    # models_ = hlib.reduce_paths_by_model(paths = paths_, models = models_, tests = tests, print_opt = print_opt)


    # # Load Hibou path data to storage S3 bucket
    # # url = f"http://10.64.18.171:9000/minio/aske/research/BIO/dist/v4.0/hibou/{model['id_emmaa']}/paths_{test['id_emmaa]}.jsonl"
    # logger.info('Loading reduced paths to storage S3 bucket...')
    # s3_path_storage_paths_hibou = hlib.build_path_iter.map(
    #     path = unmapped(s3_path_storage),
    #     path_ = unmapped(f'/paths.jsonl'),
    #     path_iter = models_, 
    #     key_iter = unmapped('id_emmaa'),
    #     print_opt = unmapped(print_opt)
    # )
    # preamble = hlib.get_obj_preamble(obj_type = 'paths')
    # hlib.load_obj_to_s3.map(
    #     obj = models_, 
    #     s3_url = unmapped(s3_url), 
    #     s3_bucket = unmapped(s3_bucket), 
    #     s3_path = s3_path_storage_paths_hibou, 
    #     preamble = unmapped(preamble),
    #     obj_key = unmapped('paths')
    # )


    # # Compute `tested` status of model edges using `paths`
    # models_ = hlib.test_edges.map(model = models_, print_opt = unmapped(print_opt))


    # # Load model edges to storage S3 bucket
    # logger.info('Loading model edges to storage S3 bucket...')
    # s3_path_storage_model_edges = hlib.build_path_iter.map(
    #     path = unmapped(s3_path_storage),
    #     path_ = unmapped(f'/edges.jsonl'),
    #     path_iter = models_, 
    #     key_iter = unmapped('id_emmaa'),
    #     print_opt = unmapped(print_opt)
    # )
    # preamble = hlib.get_obj_preamble(obj_type = 'edges')
    # hlib.load_obj_to_s3.map(
    #     obj = models_, 
    #     s3_url = unmapped(s3_url), 
    #     s3_bucket = unmapped(s3_bucket), 
    #     s3_path = s3_path_storage_model_edges, 
    #     preamble = unmapped(preamble),
    #     obj_key = unmapped('edges')
    # )


    # # Generate node layout


    # # Load node layout to storage S3 bucket


    # Preprocess ontology and model node groundings:
    # * Remove all `xref` links in the ontology
    # * Generate ordered namespace list for the model node groundings
    # * Sort model node groundings according to given priority (HMS)
    # * Check existence of node groundings in the ontology
    logger.info('Preprocess ontology and model node groundings...')
    ontology_ = hlib.transform_ontology(
        ontology = ontology,
        print_opt = print_opt
    )
    models_ = hlib.generate_ordered_namespace_list.map(
        model = models_, 
        ontology = unmapped(ontology_), 
        namespaces_priority = unmapped(namespaces_priority), 
        print_opt = unmapped(print_opt)
    )
    models_ = hlib.sort_nodes_groundings.map(
        model = models_
    )
    models_ = hlib.ground_nodes_to_ontology.map(
        model = models_, 
        ontology = unmapped(ontology_),
        print_opt = unmapped(print_opt)
    )


    # # Load model nodes to storage S3 bucket
    # logger.info('Loading model nodes to storage S3 bucket...')
    # s3_path_storage_model_nodes = hlib.build_path_iter.map(
    #     path = unmapped(s3_path_storage),
    #     path_ = unmapped(f'/nodes.jsonl'),
    #     path_iter = models_, 
    #     key_iter = unmapped('id_emmaa'),
    #     print_opt = unmapped(print_opt)
    # )
    # preamble = hlib.get_obj_preamble(obj_type = 'nodes')
    # hlib.load_obj_to_s3.map(
    #     obj = models_, 
    #     s3_url = unmapped(s3_url), 
    #     s3_bucket = unmapped(s3_bucket), 
    #     s3_path = s3_path_storage_model_nodes, 
    #     preamble = unmapped(preamble),
    #     obj_key = unmapped('nodes')
    # )


    # Compute ontology ancestry (shortest-to-ancestor path) of all model nodes
    logger.info('Compute ontology ancestry of model nodes groundings...')
    models_ = hlib.compute_onto_ancestry.map(
        model = models_, 
        ontology = unmapped(ontology),
        print_opt = unmapped(print_opt)    
    )


    # Generate `groups` from the ontology ancestry of all model nodes
    logger.info('Generate ontology groups from model node groundings...')
    models_ = hlib.generate_onto_groups.map(
        model = models_,
        ontology = unmapped(ontology),
        print_opt = unmapped(print_opt)
    )


    # Load groups to storage S3 bucket
    logger.info('Loading ontology groups to storage S3 bucket...')
    s3_path_storage_model_groups = hlib.build_path_iter.map(
        path = unmapped(s3_path_storage),
        path_ = unmapped(f'/groups.jsonl'),
        path_iter = models_, 
        key_iter = unmapped('id_emmaa'),
        print_opt = unmapped(print_opt)
    )
    preamble = hlib.get_obj_preamble(obj_type = 'groups')
    hlib.load_obj_to_s3.map(
        obj = models_, 
        s3_url = unmapped(s3_url), 
        s3_bucket = unmapped(s3_bucket), 
        s3_path = s3_path_storage_model_groups, 
        preamble = unmapped(preamble),
        obj_key = unmapped('groups')
    )


    # # Load model `nodeAtts` to storage S3 bucket
    # logger.info('Loading model node attributes to storage S3 bucket...')
    # s3_path_storage_model_nodeAtts = hlib.build_path_iter.map(
    #     path = unmapped(s3_path_storage),
    #     path_ = unmapped(f'/nodeAtts.jsonl'),
    #     path_iter = models_, 
    #     key_iter = unmapped('id_emmaa'),
    #     print_opt = unmapped(print_opt)
    # )
    # preamble = hlib.get_obj_preamble(obj_type = 'nodeAtts')
    # hlib.load_obj_to_s3.map(
    #     obj = models_, 
    #     s3_url = unmapped(s3_url), 
    #     s3_bucket = unmapped(s3_bucket), 
    #     s3_path = s3_path_storage_model_nodeAtts, 
    #     preamble = unmapped(preamble),
    #     obj_key = unmapped('nodeAtts')
    # )


    # Generate group layout


    # Load group layout to storage S3 bucket




# %%[markdown]
# # Run Flow

# %%[markdown]
# ## Visualize Flow

# `flow.visualize()`

# %%[markdown]
# ## Run Locally

state = flow.run(executor = prefect.executors.LocalExecutor(), todo_models = 'aml', print_opt = False)

# %%[markdown]
# ## Register the Flow

# flow.register(project_name = 'aske')


