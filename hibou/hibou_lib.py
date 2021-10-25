# %%[markdown]
# Author: Nelson Liu 
#
# Email: [nliu@uncharted.software](mailto:nliu@uncharted.software)

# %%[markdown]
# Content: Define Prefect Tasks

# %%[markdown]
# # Import Modules

import numpy as np
import networkx as nx
import requests
import datetime
import json
import re
import gzip
from io import StringIO
from typing import Dict, List, Tuple, Set, Union, Optional, NoReturn

import boto3
from botocore.client import Config

import prefect
from prefect import task, Flow, Parameter, case
from prefect.tasks.control_flow import merge
from prefect.client.secrets import Secret

# %%
np.random.seed(0)

# %%
# Build path
@task
def build_path(path: List[Union[str, Dict]], keys: Optional[List[str]] = None, print_opt: bool = False) -> str:

    full_path = ''
    for i, p in enumerate(path):

        if isinstance(p, dict):
            full_path = full_path + p[keys[i]]

        else:
            full_path = full_path + p


    # Message
    if print_opt == True:
        logger = prefect.context.get('logger')
        logger.info(f'Path: {full_path}')

    return full_path

# %%
# Iteratively build path
@task
def build_path_iter(path: str, path_: str, path_iter: Union[str, Dict], key_iter: Optional[str] = None, print_opt: bool = False) -> str:

    full_path = ''
    if isinstance(path_iter, Dict) & (key_iter != None):
        full_path = path + path_iter[key_iter] + path_

    else:
        full_path = path + path_iter + path_


    if print_opt == True:
        logger = prefect.context.get('logger')
        logger.info(f'Path: {full_path}')


    return full_path


# %%
# Get the preamble of a given object type
@task
def get_obj_preamble(obj_type: str) -> Dict:

    preamble = {}
    obj_types_valid = ('models', 'tests', 'paths', 'edges', 'evidences', 'docs', 'docLayout', 'nodes', 'nodeLayout', 'nodeAtts', 'groups', 'groupLayout')
    if obj_type not in obj_types_valid:
        raise ValueError(f'`{obj_type}` is not a valid object type.')


    if obj_type == 'models':
    
        preamble = {
            'id': '<int> ID of this model',
            'id_emmaa': '<str> EMMAA ID of this model, necessary for making requests on the EMMAA API',
            'name': '<str> Human-readable name of this model',
            'description': '<str> Human-readable description of this model',
            'test_ids': '<list of ints> list ofIDs of the tests against which this model has been tested by EMMAA',
            'snapshot_time': '<str> Date and UTC time (ISO 8601 format) at which the model data (statements, etc.) is requested on the EMMAA API'
        }


    if obj_type == 'tests':

        preamble = {
            'id': '<int> ID of this test (corpus)',
            'id_emmaa': '<str> EMMAA ID of this test, necessary for making requests on the EMMAA API',
            'name': '<str> Human-readable name of this test',
            'model_ids': '<list of ints> list ofIDs of the models that have been tested against this test by EMMAA',
            'snapshot_time': '<str> Date and UTC time (ISO 8601 format) at which the model data (statements, etc.) is requested on the EMMAA API'
        }


    if obj_type == 'paths':

        preamble = {
            'id': '<int> ID of this (test or explanatory) path',
            'model_id': '<int> ID of the associated model',
            'test_id': '<int> ID of the associated test (corpus)',
            'test_statement_id': '<str> EMMAA ID of the test statement that this path explains',
            'type': '<str> type of this path (`unsigned_graph`, `signed_graph`, `pybel`, etc.)',
            'edge_ids': '<int> List of IDs of the edges in this path',
            'node_ids': '<int> List of IDs of the nodes in this path'
        }


    if obj_type == 'edges':

        preamble = {
            'id': '<int> ID of this edge',
            'model_id': '<int> ID of the associated model',
            'statement_id': '<str> EMMAA ID or `matches_hash` of the INDRA statement from which this edge is derived',
            'statement_type': '<str> type of the source statement',
            'belief': '<float> belief score of the source statement',
            'evidence_ids': '<list of ints> list of IDs of the evidences that support the source statement',
            'doc_ids': '<list of ints> list of IDs of the docs that support the source statement',
            'source_node_id': '<int> ID of the source node',
            'target_node_id': '<int> ID of the target node',
            'tested': '<bool> test status of the source statement according to `paths`',
            'test_path_ids': '<list of ints> list of IDs of (test/explanatory) paths that reference this edge',
            'curated': '<int> curation status of the source statement',
            'directed': '<bool> whether this edge is directed or not (`True` = directed, `False` = undirected)',
            'polarity': '<bool> prescribed polarity of this edge (`True` = positive, `False` = negative, `None` = undefined)'
        }


    if obj_type == 'evidences':

        preamble = {
            'id': '<int> ID of this evidence',
            'model_id': '<int> ID of the associated model',
            'text': '<str> plain text of this evidence',
        }


    if obj_type == 'docs':

        preamble = {
            'id': '<int> ID of this doc',
            'model_id': '<int> ID of the associated model',
            'evidence_ids': '<list of ints> list ofIDs of the evidences that references this doc',
            'edge_ids': '<list of ints> list ofIDs of the edges that references this doc',
            'identifier': '<list of dicts> list ofexternal doc IDs (dict keys = `type`, `id`)',
        }


    if obj_type == 'docLayout':

        preamble = {
            'doc_id': '<int> ID of the associated doc',
            'coor_sys_name': '<str> name of the coordinate system (`cartesian`, `spherical`)',
            'coors': '<list of floats> list ofcoordinate values of this doc layout',
        }


    if obj_type == 'nodes':

        preamble = {
            'id': '<int> ID of this node',
            'model_id': '<int> ID of the associated model',
            'name': '<str> human-readable name of this node',
            'grounded_db': '<bool> whether this node is grounded to any database',
            'db_ids': '<list of strs> list of database `namespace:id` strings, sorted by priority in descending order',
            'edge_ids_source': '<list of ints> list of IDs of the edges to which this node is the source',
            'edge_ids_target': '<list of ints> list of IDs of the edges to which this node is the target',
            'out_degree': '<int> out-degree of this node (length of `edge_ids_source`)', 
            'in_degree': '<int> in-degree of this node (length of `edge_ids_target`)'
        }


    if obj_type == 'nodeLayout':

        preamble = {
            'node_id': '<int> ID of the node',
            'coor_sys_name': '<str> name of the coordinate system (`cartesian`, `spherical`)',
            'coors': '<list of floats> list of coordinate values of this node layout',
        }

    if obj_type == 'nodeAtts':

        preamble = {
            'node_id': '<int> ID of the node',
            # 'db_ref_priority': '<str>',
            'grounded_group': '<bool> whether this node is grounded to the given ontology',
            'type': '<str> `name` of the ancestor ontological group of this node',
            'group_ids': 'ordered list of IDs of the ontological groups to which database grounding of the node is mapped (order = ancestor-to-parent)',
            # 'group_refs': '<list of strs>',
            'node_group_level': '<int> length of the shortest path from the parent ontological group of the node to the ancestor group, plus one' 
        }


    if obj_type == 'groups':

        preamble = {
            'id': '<int> ID of this group',
            'id_onto': '<str> ID of this group within the given ontology (format = `namespace:id`)',
            'name': '<str> human-readable name of this group',
            'level': '<int> length of the shortest path from this group to the ancestor group',
            'parent_id': '<int> ID of other groups in the ontology that are the immediate parent of this group',
            'children_ids': '<list of ints> List of IDs of other groups in the ontology that are the immediate children of this group',
            'model_id': '<int> ID of the associated model',
            'node_ids_all': '<list of ints> List of IDs of the model nodes that are grounded to this group and all its children',
            'node_ids_direct': '<list of ints> List of IDs of the model nodes that are directly grounded to this group (i.e. excluding its children)'
        }


    if obj_type == 'groupLayout':

        preamble = {
            'node_id': '<int> ID of the group',
            'coor_sys_name': '<str> name of the coordinate system (`cartesian`, `spherical`)',
            'coors': '<list of floats> list of coordinate values of this group layout',
        }


    return preamble


# %%
# Read `todo_models` as 'all' or list of `id_emmaa` values
# todo_models: <str> space-separated list of `id_emmaa` of the models or 'all' 
@task
def read_todo_models(todo_models: str = 'all', exclude_models: str = 'food_insecurity', models: List = [], print_opt: bool = False) -> List:

    # Model exclusion
    exclude_models_list = exclude_models.split(' ')


    # Generate to-do model ID list
    if todo_models == 'all':
        todo_models_list = [model['id_emmaa'] for model in models if model['id_emmaa'] not in exclude_models_list]

    else:
        todo_models_list = [model_id for model_id in todo_models.split(' ') if model_id not in exclude_models_list]


    # Message
    if print_opt == True:
        logger = prefect.context.get('logger')
        logger.info(f'{len(todo_models_list)} model(s) to extract and process')


    return todo_models_list


# %%
# Extract model, test, path lists using EMMAA API
@task(max_retries = 1, retry_delay = datetime.timedelta(seconds = 0.5))
def extract_models_tests_paths_from_emmaa(emmaa_api_url: str, print_opt: bool = False) -> Tuple[List[Dict], List[Dict], List[Dict]]:

    # Get logger
    logger = prefect.context.get('logger')


    # Snapshot datetime
    snapshot_datetime = datetime.datetime.utcnow().replace(microsecond = 0).isoformat()


    # Get model list
    url = emmaa_api_url + '/models'
    try:
        response = requests.get(url).json()
        models = [{
            'id': i, 
            'id_emmaa': name,
            'name': None,
            'description': None,
            'tests': None,
            'test_ids': None,
            'snapshot_datetime': snapshot_datetime,
            'statements': None
        } for i, name in enumerate(response['models'])]

        if print_opt == True:
            logger.info(f'API request: {url}')

    except:

        models = []

        if print_opt == True:
            logger.info('API request error: {url}')
        


    # Get model metadata
    for model in models:

        url = emmaa_api_url + '/model_info/' + model['id_emmaa']
        try:
            response = requests.get(url).json()
            model['name'] = response['human_readable_name']
            model['description'] = response['description']

            if print_opt == True:
                logger.info(f'API request: {url}')

        except:

            model['name'] = None
            model['description'] = None

            if print_opt == True:
                logger.info(f'API request error: {url}')



        # Get list of tests against which the model is evaluated
        url = emmaa_api_url + '/test_corpora/' + model['id_emmaa']
        try:
            response = requests.get(url).json()
            model['tests'] = response['test_corpora']

            if print_opt == True:
                logger.info(f'API request: {url}')

        except:

            model['tests'] = None

            if print_opt == True:
                logger.info(f'API request error: {url}')
            


    # Transform local IDs to global IDs
    models = [transform_obj_ids(obj = model, obj_type = 'models') for model in models]


    # Build test list
    x = sorted(set([test for model in models for test in model['tests']]))
    tests = [{
        'id': i, 
        'id_emmaa': name,
        'name': None,
        'model_ids': None,
        'snapshot_datetime': snapshot_datetime,
        'statements': None
    } for i, name in enumerate(x)]


    # Transform local IDs to global IDs
    tests = [transform_obj_ids(obj = test, obj_type = 'tests') for test in tests]


    # Map tests to IDs
    map_tests_ids = {test['id_emmaa']: test['id'] for test in tests}
    for model in models:
        model['test_ids'] = [map_tests_ids[test_name] for test_name in model['tests']]


    # Get model IDs for each test
    for test in tests:
        test['model_ids'] = [model['id'] for model in models if test['id'] in model['test_ids']]


    # Get test metadata
    for test in tests:

        url = emmaa_api_url + '/tests_info/' + test['id_emmaa']
        try:
            response = requests.get(url).json()
            test['name'] = response['name']

            if print_opt == True:
                logger.info(f'API request: {url}')

        except:

            test['name'] = None

            if print_opt == True:
                logger.info(f'API request error: {url}')
           


    # Get path list
    paths = [{
        'id_emmaa_model': model['id_emmaa'],  
        'id_emmaa_test': id_emmaa_test,
        'emmaa_data': None
    } for model in models for id_emmaa_test in model['tests']]


    # Message
    if print_opt == True:

        i = 59
        print(f"{'-' * i}")
        print(f"| {'Model ID':^10} | {'EMMAA ID':^23} | {'Number of Tests':^16} |")
        print(f"{'-' * i}")
        for model in models:
            print(f"| {model['id']:^10} | {model['id_emmaa']:^23} | {len(model['tests']):^16} |")
        print(f"{'-' * i}\n")


        print(f"{'-' * i}")
        print(f"| {'Test ID':^10} | {'EMMAA ID':^23} | {'Number of Models':^16} |")
        print(f"{'-' * i}")
        for test in tests:
            print(f"| {test['id']:^10} | {test['id_emmaa']:^23} | {len(test['model_ids']):^16} |")
        print(f"{'-' * i}\n")
        

    return (models, tests, paths)

# %%
# Extract data object from a S3 bucket
@task
def extract_data(domain: str, path: str, file_ext: str, return_obj: Optional[Dict] = None, return_obj_key: Optional[str] = None, print_opt: bool = False) -> Union[List, Dict, str]:

    # Get logger
    logger = prefect.context.get('logger')


    # Get data from S3 bucket as JSON object
    url = f"{domain}/{path}"

    if print_opt == True:
        logger.info(f'API request: {url}')  

    try:
        response = requests.get(url)
        
    except:

        if print_opt == True:
            logger.info(f'Request error')

    finally:

        if print_opt == True:
            logger.info(f'Response: {response.status_code}, {response.reason}')


    # Deserialize data
    if file_ext == '.json':
        try:
            obj = response.json()

        except:
            obj = None

    elif file_ext == '.jsonl':
        try:
            obj = [json.loads(line) for line in response.text.splitlines()]

        except:
            obj = []

    elif file_ext == '.json.gz':
        try:
            obj = json.loads(gzip.decompress(response.content))

        except:
            obj = None

    else:
        try:
            obj = response.text

        except:
            obj = None


    if obj == None:
        if print_opt == True:
            logger.info(f"API request returned invalid response")
    

    # Store deserialized data
    if isinstance(return_obj, dict):
        return_obj[return_obj_key] = obj

    else: 
        return_obj = obj


    return return_obj


# %%
# Load object to S3 bucket
@task(max_retries = 1, retry_delay = datetime.timedelta(seconds = 0.5))
def load_obj_to_s3(obj: Union[List, Dict], s3_url: str, s3_bucket: str, s3_path: str, preamble: Optional[Dict] = None, obj_key: Optional[str] = None, print_opt: bool = False) -> NoReturn:

    # Get logger
    logger = prefect.context.get('logger')


    # Pick out sub-object by key
    if isinstance(obj, dict) & (obj_key != None):
        obj = obj[obj_key]


    # Serialize `obj`
    with StringIO() as fp:

        # `obj` is type `dict`
        if isinstance(obj, dict):

            # Only include keys specified by preamble
            if preamble != None:
                # obj_ = {k: v for k, v in obj.items() if k in preamble.keys()}
                obj_ = {k: obj[k] if k in obj.keys() else None for k in preamble.keys() if k in preamble.keys()}

            else:
                obj_ = obj


            json.dump(obj_, fp)


        # `obj` is type `list` of `dict`
        if isinstance(obj, list):

            # Include preamble if available
            if preamble != None:
                json.dump(preamble, fp)
                fp.write('\n')


            # Do for each 
            for item in obj:

                # Only include keys specified by preamble
                if preamble != None:
                    # item_ = {k: v for k, v in item.items() if k in preamble.keys()}
                    item_ = {k: item[k] if k in item.keys() else None for k in preamble.keys()}

                else:
                    item_ = item


                json.dump(item_, fp)
                fp.write('\n')


        # Load file onto S3 bucket
        try:

            # Define S3 interface
            s3_resource = boto3.resource(
                's3', 
                endpoint_url = s3_url, 
                config = Config(signature_version = 's3v4'), 
                region_name = 'us-east-1',
                aws_access_key_id = 'foobar',
                aws_secret_access_key = 'foobarbaz',
            )
            # aws_access_key_id = Secret('AWS_CREDENTIALS').get()['ACCESS_KEY']
            # aws_secret_access_key = Secret('AWS_CREDENTIALS').get()['SECRET_ACCESS_KEY']


            s3_resource.Object(s3_bucket, s3_path).put(Body = fp.getvalue())


            if print_opt == True:
                logger.info(f'S3 request: {s3_url}/{s3_bucket}/{s3_path}')

        except:

            if print_opt == True:
                logger.info(f'S3 request error: {s3_url}/{s3_bucket}/{s3_path}')


# %%
# Filter objects by key values
@task
def filter_objs(objs: List[Dict], obj_key: str, obj_vals: Union[List, Tuple, Set]) -> List[Dict]:

    return [obj for obj in objs if obj[obj_key] in obj_vals]


# %%
# Transform object IDs into globally unique IDs
def transform_obj_ids(obj: Union[Dict, int], obj_type: str, obj_id_key: str = 'id', num_bits_global: int = 32, num_bits_namespace: int = 4, reverse: bool = False) -> Dict:

    # Number of bits for the local ID
    num_bits_local = num_bits_global - num_bits_namespace


    # Local ID -> globally unique ID
    if reverse == False:

        # Object type -> namespace ID
        obj_types = ('models', 'tests', 'paths', 'edges', 'evidences', 'docs', 'nodes', 'groups')
        map_type_namespace = {t: i for i, t in enumerate(obj_types)}
        id_namespace = map_type_namespace[obj_type]


        if isinstance(obj, dict):
            id_local = obj[obj_id_key]
            id_global = (id_namespace << num_bits_local) | id_local
            obj[obj_id_key] = id_global
        
        elif isinstance(obj, int):
            id_local = obj
            id_global = (id_namespace << num_bits_local) | id_local
            obj = id_global


    # globally unique ID -> local ID
    else:

        if isinstance(obj, dict):
            id_global = obj[obj_id_key]

        elif isinstance(obj, int):
            id_global = obj


        # a = 0xf0000000
        a = int((1.0 - 2.0 ** num_bits_namespace) / (1.0 - 2.0)) << num_bits_local
        id_namespace = (id_global & a) >> num_bits_local


        # b = 0x0fffffff
        b = int(2.0 ** num_bits_local - 1.0)
        id_local = (id_global & b)
        obj[obj_id_key] = id_local


        if isinstance(obj, dict):
            obj[obj_id_key] = id_local
        
        elif isinstance(obj, int):
            obj = id_local


    return obj

# %%
# Transform EMMAA statements of each model into node, edge lists
@task
def transform_statements_to_node_edge_lists(model: List[Dict], print_opt: bool = False) -> List[Dict]:

    # Get logger
    logger = prefect.context.get('logger')


    # Initialize
    statements = model['statements']
    num_statements = len(statements)
    statements_processed = []
    nodes = []
    edges = []
    evidences = []
    docs = []
    

    if num_statements > 0:

        # Only keep statements that can be clearly represented by a directed edge
        source_target_pairs = [
            ('subj', 'obj'), 
            ('enz', 'sub'), 
            ('gef', 'ras'),
            ('gap', 'ras'),
            ('subj', 'obj_from', 'obj_to'), 
            ('members')]
        bool_ind = np.sum(np.array([[True if set(p) <= set(s.keys()) else False for s in statements] for p in source_target_pairs]), axis = 0)
        statements_processed = [s for i, s in zip(bool_ind, statements) if i]
        # num_statements_processed = len(statements_processed)


        statements = None
        del statements


        # Extract edge list
        edge = []
        for s in statements_processed:
            

            # Get statement entity roles
            k = [i for i, p in enumerate(source_target_pairs) if set(p) <= set(s.keys())]

            
            # None
            if len(k) < 1:

                pass


            # subj -> obj 
            elif k[0] == 0:

                src = source_target_pairs[k[0]][0]
                tgt = source_target_pairs[k[0]][1]

                edge = [{
                    'id': None, 
                    'model_id': model['id'],
                    'statement_id': str(s['matches_hash']), 
                    'statement_type': str(s['type']), 
                    'belief': float(s['belief']), 
                    'evidence_ids': None,
                    'doc_ids': None,
                    'source_node_id': None, 
                    'source_node_name': str(s[src]['name']),
                    'source_node_db_refs': s[src]['db_refs'],
                    'target_node_id': None, 
                    'target_node_name': str(s[tgt]['name']),
                    'target_node_db_refs': s[tgt]['db_refs'], 
                    'tested': None,
                    'curated': None,
                    'directed': True,
                    'polarity': (str(s['type']) == 'Activation') | (str(s['type']) == 'IncreaseAmount')
                }]


            # enz -> sub
            # gef -> ras
            # gap -> ras
            elif k[0] in [1, 2, 3]:

                src = source_target_pairs[k[0]][0]
                tgt = source_target_pairs[k[0]][1]

                edge = [{
                    'id': None, 
                    'model_id': model['id'],
                    'statement_id': str(s['matches_hash']), 
                    'statement_type': str(s['type']), 
                    'belief': float(s['belief']), 
                    'evidence_ids': None,
                    'doc_ids': None,
                    'source_node_id': None, 
                    'source_node_name': str(s[src]['name']),
                    'source_node_db_refs': s[src]['db_refs'],
                    'target_node_id': None, 
                    'target_node_name': str(s[tgt]['name']),
                    'target_node_db_refs': s[tgt]['db_refs'], 
                    'tested': None,
                    'curated': None,
                    'directed': True,
                    'polarity': None
                }]


            # subj -> obj_from
            # subj -> obj_to
            elif k[0] == 4:

                src = source_target_pairs[k[0]][0]
                tgt_1 = source_target_pairs[k[0]][1]
                tgt_2 = source_target_pairs[k[0]][2]

                edge = [{
                    'id': None, 
                    'model_id': model['id'],
                    'statement_id': str(s['matches_hash']), 
                    'statement_type': str(s['type']), 
                    'belief': float(s['belief']), 
                    'evidence_ids': None,
                    'doc_ids': None,
                    'source_node_id': None, 
                    'source_node_name': str(s[src]['name']),
                    'source_node_db_refs': s[src]['db_refs'],
                    'target_node_id': None, 
                    'target_node_name': str(s[tgt_1][0]['name']),
                    'target_node_db_refs': s[tgt_1][0]['db_refs'],
                    'tested': None,
                    'curated': None,
                    'directed': True,
                    'polarity': False
                }, 
                {
                    'id': None, 
                    'model_id': model['id'],
                    'statement_id': str(s['matches_hash']), 
                    'statement_type': str(s['type']), 
                    'belief': float(s['belief']), 
                    'evidence_ids': None,
                    'doc_ids': None,
                    'source_node_id': None, 
                    'source_node_name': str(s[src]['name']),
                    'source_node_db_refs': s[src]['db_refs'],
                    'target_node_id': None, 
                    'target_node_name': str(s[tgt_2][0]['name']),
                    'target_node_db_refs': s[tgt_2][0]['db_refs'],
                    'tested': None,
                    'curated': None,
                    'directed': True,
                    'polarity': True
                }]


            # many-member statements
            # * assume bidirectionality
            elif k[0] == 5:
                
                if len(s['members']) > 0:

                    num_members = len(s['members'])
                    perm = [(i, j) for i in range(num_members) for j in range(num_members) if i != j]


                    edge = [{
                        'id': None, 
                        'model_id': model['id'],
                        'statement_id': str(s['matches_hash']), 
                        'statement_type': str(s['type']), 
                        'belief': float(s['belief']), 
                        'evidence_ids': None,
                        'doc_ids': None,
                        'source_node_id': None, 
                        'source_node_name': str(s['members'][x[0]]['name']), 
                        'source_node_db_refs': s['members'][x[0]]['db_refs'], 
                        'target_node_id': None, 
                        'target_node_name': str(s['members'][x[1]]['name']),
                        'target_node_db_refs': s['members'][x[1]]['db_refs'],
                        'tested': None,
                        'curated': None,
                        'directed': True,
                        'polarity': None
                    } for x in perm]


            edges.extend(edge)
            edge = []


        # Generate local edge IDs
        num_edges = len(edges)
        for i, edge in enumerate(edges):
            edge['id'] = i


        # Transform local IDs to global IDs
        edges = [transform_obj_ids(obj = edge, obj_type = 'edges') for edge in edges]


        # Map statements to edges
        map_statements_edges = {edge['statement_id']: [] for edge in edges}
        for edge in edges:
            if edge['statement_id'] in map_statements_edges.keys():
                map_statements_edges[edge['statement_id']].append(edge['id'])


        # #####################

        # Extract evidence list
        evidences_all = []
        for s in statements_processed:

            evidence = []

            if 'evidence' in s.keys():

                for ev in s['evidence']:

                    evidence = {
                        'text': None,
                        'text_refs': None, 
                        'source_hash': str(ev['source_hash']), 
                        'statement_id': str(s['matches_hash'])
                    }

                    if 'text' in ev.keys():
                        evidence['text'] = ev['text']

                    if 'text_refs' in ev.keys():
                        evidence['text_refs'] = ev['text_refs']

                    evidences_all.append(evidence)


        # De-duplicate evidence list by `source_hash`
        map_evidences_statements = {ev['source_hash']: [] for ev in evidences_all}
        for ev in evidences_all:
            map_evidences_statements[ev['source_hash']].append(ev['statement_id'])


        # Generate list of unique evidences
        map_evidences_data = {ev['source_hash']: ev for ev in evidences_all}
        evidences = [{
            'id': i, 
            'model_id': model['id'],
            'text': map_evidences_data[k]['text'],
            'text_refs': map_evidences_data[k]['text_refs'], 
            'source_hash': map_evidences_data[k]['source_hash'], 
            'statement_ids': map_evidences_statements[k],
            'edge_ids': [edge for s in map_evidences_statements[k] for edge in map_statements_edges[s]],
            'doc_ids': None 
        } for i, k in enumerate(map_evidences_statements.keys())]
        num_evidences = len(evidences)

        del evidences_all, map_evidences_data


        # Transform local IDs to global IDs
        evidences = [transform_obj_ids(obj = ev, obj_type = 'evidences') for ev in evidences]


        # Get evidence IDs for `edges`
        map_edges_evidences = {edge['id']: [] for edge in edges}
        for ev in evidences:
            for s in ev['statement_ids']:
                for edge_id in map_statements_edges[s]:
                    map_edges_evidences[edge_id].append(ev['id'])

        for edge in edges:
            edge['evidence_ids'] = map_edges_evidences[edge['id']]
            

        #####################

        # Hash `text_refs` to de-duplicate docs referenced by `evidences`
        map_docs_textrefs = {hash(repr(ev['text_refs'])): ev['text_refs'] for ev in evidences if ev['text_refs'] != None}
        map_docs_evidences = {hash(repr(ev['text_refs'])): [] for ev in evidences if ev['text_refs'] != None}
        map_docs_edges = {hash(repr(ev['text_refs'])): [] for ev in evidences if ev['text_refs'] != None}


        # Map docs to evidence lists
        for ev in evidences:

            if ev['text_refs'] != None:

                h = hash(repr(ev['text_refs']))
                map_docs_evidences[h].append(ev['id'])

                for s in ev['statement_ids']:
                    map_docs_edges[h] = map_docs_edges[h] + map_statements_edges[s]


        # Extract document list
        docs = [{
            'id': i,
            'model_id': model['id'],
            'evidence_ids': map_docs_evidences[h],
            'edge_ids': map_docs_edges[h], 
            'identifier': [{'type': k.lower(), 'id': v} for k, v in map_docs_textrefs[h].items()]
        } for i, h in enumerate(map_docs_textrefs.keys())]
        num_docs = len(docs)


        # Transform local IDs to global IDs
        docs = [transform_obj_ids(obj = doc, obj_type = 'docs') for doc in docs]


        # #####################

        # Get doc IDs for `edges`
        map_edges_docs = {edge_id: [] for doc in docs for edge_id in doc['edge_ids']}
        for doc in docs:
            for edge_id in doc['edge_ids']:
                map_edges_docs[edge_id].append(doc['id'])

        for edge in edges:
            if edge['id'] in map_edges_docs.keys():
                edge['doc_ids'] = map_edges_docs[edge['id']]


        #####################

        # Extract node list
        nodes_name = {
            **{edge['source_node_name']: {'edge_ids_source': [], 'edge_ids_target': []} for edge in edges}, 
            **{edge['target_node_name']: {'edge_ids_source': [], 'edge_ids_target': []} for edge in edges}
        }


        for edge in edges:
            nodes_name[edge['source_node_name']]['db_refs'] = edge['source_node_db_refs']
            nodes_name[edge['target_node_name']]['db_refs'] = edge['target_node_db_refs']
            nodes_name[edge['source_node_name']]['edge_ids_source'].append(edge['id'])
            nodes_name[edge['target_node_name']]['edge_ids_target'].append(edge['id'])


        nodes = [{
            'id': i,
            'model_id': model['id'],
            'name': name, 
            'grounded_db': len(set(nodes_name[name]['db_refs'].keys()) - {'TEXT'} - {'TEXT_NORM'}) > 0, 
            'db_ids': [{'namespace': k, 'id': v} for k, v in nodes_name[name]['db_refs'].items() if (k != 'TEXT') & (k != 'TEXT_NORM')],
            'edge_ids_source': nodes_name[name]['edge_ids_source'], 
            'edge_ids_target': nodes_name[name]['edge_ids_target'],
            'in_degree': len(nodes_name[name]['edge_ids_target']),
            'out_degree': len(nodes_name[name]['edge_ids_source'])
        } for i, name in enumerate(nodes_name)]
        num_nodes = len(nodes)


        # Transform local IDs to global IDs
        nodes = [transform_obj_ids(obj = node, obj_type = 'nodes') for node in nodes]

        
        # Map node names to IDs
        map_names_nodes = {node['name']: node['id'] for node in nodes}
        for edge in edges:
            edge['source_node_id'] = map_names_nodes[edge['source_node_name']]
            edge['target_node_id'] = map_names_nodes[edge['target_node_name']]


    if print_opt == True:
        logger.info(f"{num_statements} statements -> {num_edges} edges, {num_nodes} nodes, {num_evidences} evidences, {num_docs} docs")


    model['edges'] = edges
    model['nodes'] = nodes
    model['evidences'] = evidences
    model['docs'] = docs

    return model

# %%
# Transform EMMAA paths into Hibou format
@task
def transform_path_to_hibou_iter(path: Dict, models: List[Dict], tests: List[Dict], path_data_key: str = 'emmaa_data', model_edges_key: str = 'edges', model_nodes_key: str = 'nodes') -> Dict:

    # Validate arguments
    if path_data_key not in path.keys():
        raise KeyError(f'`{path_data_key}` is not a valid key of the `path` object.')


    # Map EMMAA IDs to Hibou IDs
    map_models = {model['id_emmaa']: i for i, model in enumerate(models)}
    map_tests = {test['id_emmaa']: i for i, test in enumerate(tests)}


    # Get relevant model object
    model = models[map_models[path['id_emmaa_model']]]
    test = tests[map_tests[path['id_emmaa_test']]]


     # Validate arguments
    if model_edges_key not in model.keys():
        raise KeyError(f'`{model_edges_key}` is not a valid key of the `model` object.')

    if model_nodes_key not in model.keys():
        raise KeyError(f'`{model_nodes_key}` is not a valid key of the `model` object.')


    # Map node-names to IDs
    map_nodes_ids = {node['name']: node['id'] for node in model[model_nodes_key]}
    map_edges_ids = {edge['statement_id']: edge['id'] for edge in model[model_edges_key]}


    # Generate Hibou path data
    hibou_data = [{
        'id': i,
        'model_id': model['id'],
        'test_id': test['id'], 
        'test_statement_id': str(path_statement['test']),
        'edge_ids': [map_edges_ids[str(h)] for s in path_statement['edges'] if s['type'] == 'statements' for h in s['hashes'] if str(h) in map_edges_ids.keys()],
        'node_ids': [map_nodes_ids[node_name] for node_name in path_statement['nodes'] if node_name in map_nodes_ids.keys()]
    } for i, path_statement in enumerate(path['emmaa_data'])]


    # Transform local IDs to global IDs
    hibou_data = [transform_obj_ids(obj = path_statement, obj_type = 'paths') for path_statement in hibou_data]


    path['hibou_data'] = hibou_data


    return path


# %%
# Transform EMMAA paths into Hibou format
@task
def reduce_paths_by_model(paths: List[Dict], models: List[Dict], tests: List[Dict], path_data_key: str = 'emmaa_data', model_edges_key: str = 'edges', model_nodes_key: str = 'nodes', print_opt: bool = False) -> Dict:

     # Validate arguments
    if (len(paths) > 0) & (path_data_key not in paths[0].keys()):
        raise KeyError(f'`{path_data_key}` is not a valid key of the `paths` object.')

    if len(models) > 0:

        if model_edges_key not in models[0].keys():
            raise KeyError(f'`{model_edges_key}` is not a valid key of the `models` object.')

        if model_nodes_key not in models[0].keys():
            raise KeyError(f'`{model_nodes_key}` is not a valid key of the `models` object.')


    # Map paths to models
    map_models_paths = {path['id_emmaa_model']: i for i, path in enumerate(paths)}


    # Map test EMMAA ID to Hibou ID
    map_tests_ids = {test['id_emmaa']: test['id'] for test in tests}


    # Reduce paths by model
    for model in models:

        # Get relevant paths
        paths_model = [path for path in paths if path['id_emmaa_model'] == model['id_emmaa']]


        if len(paths_model) > 0:

            # Map node-names to IDs
            map_nodes_ids = {node['name']: node['id'] for node in model[model_nodes_key]}
            map_edges_ids = {edge['statement_id']: edge['id'] for edge in model[model_edges_key]}

            
            # paths_data = []
            # for path in paths_model:
            #     for path_obj in path[path_data_key]:

            #         paths_data.append({
            #             'id': None,
            #             'model_id': model['id'],
            #             'test_id': map_tests_ids[path['id_emmaa_test']], 
            #             'test_statement_id': str(path_obj['test']),
            #             'type': path_obj['graph_type'],
            #             'edge_ids': [map_edges_ids[str(h)] for s in path_obj['edges'] if s['type'] == 'statements' for h in s['hashes'] if str(h) in map_edges_ids.keys()],
            #             'node_ids': [map_nodes_ids[node_name] for node_name in path_obj['nodes'] if node_name in map_nodes_ids.keys()]
            #         })


            # Concatenate path data by model, over tests
            paths_data = [{
                'id': None,
                'model_id': model['id'],
                'test_id': map_tests_ids[path['id_emmaa_test']], 
                'test_statement_id': str(path_obj['test']),
                'type': path_obj['graph_type'],
                'edge_ids': [map_edges_ids[str(h)] for s in path_obj['edges'] if s['type'] == 'statements' for h in s['hashes'] if str(h) in map_edges_ids.keys()],
                'node_ids': [map_nodes_ids[node_name] for node_name in path_obj['nodes'] if node_name in map_nodes_ids.keys()]
            } for path in paths_model for path_obj in path[path_data_key]]


            # Generate IDs
            for i, p in enumerate(paths_data):

                # Local IDs
                p['id'] = i

                # Transform local IDs to global IDs
                p = transform_obj_ids(obj = p, obj_type = 'paths')
        
        else:

            paths_data = []

        # Output
        model['paths'] = paths_data


        # Message
        if print_opt == True:

            logger = prefect.context.get('logger')
            logger.info(f"Model '{model['id_emmaa']}': {len(model['paths'])} paths across {len(model['test_ids'])} test(s)")

            
    return models


# %%
# Compute 'tested' status and find associated paths for all model edges
@task
def test_edges(model: Dict, model_paths_key: str = 'paths', model_edges_key: str = 'edges', print_opt: bool = False) -> Dict:

    # Get logger
    logger = prefect.context.get('logger')


    # Validate arguments
    if model_paths_key not in model.keys():
        raise KeyError(f'`{model_paths_key}` is not a valid key of the `model` object.')

    if model_edges_key not in model.keys():
        raise KeyError(f'`{model_edges_key}` is not a valid key of the `model` object.')


    # Map tested edges to paths
    map_edges_paths = {edge_id: [] for path_statement in model[model_paths_key] for edge_id in path_statement['edge_ids']}
    for path_statement in model[model_paths_key]:
        for edge_id in path_statement['edge_ids']:
            map_edges_paths[edge_id].append(path_statement['id'])


    # Check model edges for match in test paths
    for edge in model[model_edges_key]:

        if edge['id'] in map_edges_paths.keys():
            edge['tested'] = True
            edge['test_path_ids'] = map_edges_paths[edge_id]

        else:
            edge['tested'] = False
            edge['test_paths_ids'] = []
    

    # Message
    if print_opt == True:
        logger.info(f"Model {model['id_emmaa']}: {len(map_edges_paths)} tested edges")


    return model


# %% 
# Compute 'curated' status for all model edges
@ task
def curate_edge_list(model: Dict, model_edges_key: str = 'edges', model_curation_key: str = 'curation', print_opt: bool = False) -> List[Dict]:

    # Validate arguments
    if model_edges_key not in model.keys():
        raise KeyError(f'`{model_edges_key}` must be a valid key of the `model` object.')

    if model_curation_key not in model.keys():
        raise KeyError(f'`{model_curation_key}` must be a valid key of the `model` object.')


    # Value for each curation status
    map_status_value = {
        'incorrect': 0,
        'correct': 1,
        'partial': 2,
        'uncurated': 3
    }


    # Statement -> curation status
    map_curation = {s_id: status for status, l in model[model_curation_key].items() for s_id in l}


    # Check curation status for all model edges
    for edge in model[model_edges_key]:

        try:
            edge['curated'] = map_status_value[map_curation[edge['statement_id']]]

        except KeyError:
            edge['curated'] = 3


    return model


# %%
# Transform given ontology
@task
def transform_ontology(ontology: Dict, ontology_nodes_key: str = 'nodes', ontology_links_key: str = 'links', print_opt: bool = False) -> Dict:

     # Validate arguments
    if {'nodes', 'links', 'directed'} <= set(ontology.keys()): 
         pass
    else: 
        raise TypeError(f'The given `ontology` is not a valid NetworkX JSON-graph object.')

    if ontology_links_key not in ontology.keys():
        raise KeyError(f'`{ontology_links_key}` is not a valid key of the `ontology` object.')

    if ontology_links_key not in ontology.keys():
        raise KeyError(f'`{ontology_links_key}` is not a valid key of the `ontology` object.')


    # Remove all `xref` links
    ontology['links'] = [link for link in ontology[ontology_links_key] if link['type'] != 'xref']


    if print_opt == True:

        # Get logger
        logger = prefect.context.get('logger')

        logger.info(f"Ontology size: {len(ontology[ontology_nodes_key])} nodes and {len(ontology[ontology_links_key])} links")

    return ontology


# %% 
# Generate a list of grounding/ontology namespaces, ordered by the given priority (from HMS)
@task
def generate_ordered_namespace_list(model: List[Dict], ontology: Dict, namespaces_priority: str, model_nodes_key: str = 'nodes', ontology_nodes_key: str = 'nodes', print_opt: bool = False) -> Dict:

     # Validate arguments
    if {'nodes', 'links', 'directed'} <= set(ontology.keys()): 
         pass
    else: 
        raise TypeError(f'The given `ontology` is not a valid NetworkX JSON-graph object.')
    
    if model_nodes_key not in model.keys():
        raise KeyError(f'`{model_nodes_key}` is not a valid key of the `model` object.')

    if ontology_nodes_key not in ontology.keys():
        raise KeyError(f'`{ontology_nodes_key}` is not a valid key of the `ontology` object.')


    # Convert string into list
    namespaces_priority = namespaces_priority.split(' ')


    # Find all namespaces referenced in the ontology node names and model `db_ids`
    namespaces_onto = set([re.findall('\w{1,}(?=:)', node['id'])[0] for node in ontology[ontology_nodes_key]])
    namespaces_model = set([namespace_id['namespace'] for node in model[model_nodes_key] for namespace_id in node['db_ids']])


    # Combine the namespace lists in order: 
    # * given priority
    # * 'not-grounded'
    # * from model
    # * from ontology
    x = sorted(list(namespaces_model - set(namespaces_priority)))
    y = sorted(list(namespaces_onto - set(namespaces_priority) - namespaces_model))
    namespaces_ordered = namespaces_priority + x + y


    # Message
    if print_opt == True:

        # Count occurrences in model groundings
        namespaces_count = {namespace: [0, 0] for namespace in namespaces_ordered}
        for node in model[model_nodes_key]:
            for k_v in node['db_ids']:
                namespaces_count[k_v['namespace']][0] += 1

        
        # Count occurrences in ontology node names
        for node in ontology['nodes']:
            name = re.findall('\w{1,}(?=:)', node['id'])[0]
            namespaces_count[name][1] += 1


        # Message
        logger = prefect.context.get('logger')
       
        print(f"\nModel {model['id_emmaa']}:")
        print(f"{'-' * 46}")
        print(f"| {'Namespace':^20} | {'Model':^8} | {'Ontology':^8} |")
        print(f"{'-' * 46}")
        for namespace, counts in namespaces_count.items():
            print(f"| {namespace:^20} | {counts[0]:>8} | {counts[1]:>8} |")
        print(f"{'-' * 46}\n")

    # Output
    model['namespaces_ordered'] = namespaces_ordered


    return model


# %%
# Sort the `db_ids` of each model node according to the ordered namespace list
@task
def sort_nodes_groundings(model: List[Dict], model_nodes_key: str = 'nodes', model_namespaces_key: str = 'namespaces_ordered', print_opt: bool = False) -> Dict:

     # Validate arguments
    if model_nodes_key not in model.keys():
        raise KeyError(f'`{model_nodes_key}` is not a valid key of the `model` object.')
    
    if model_namespaces_key not in model.keys():
        raise KeyError(f'`{model_namespaces_key}` is not a valid key of the `model` object.')

    
    # List `db_ids` according to the ordered namespace list
    for node in model[model_nodes_key]:

        db_ids = {namespace_id['namespace']: namespace_id['id'] for namespace_id in node['db_ids']}
        node['db_ids'] = [{'namespace': k, 'id': db_ids[k]} for k in model[model_namespaces_key] if k in db_ids.keys()]


    return model


# %%
# Check whether each model node can be grounded to the given ontology
@task
def ground_nodes_to_ontology(model: List[Dict], ontology: Dict, model_nodes_key: str = 'nodes', ontology_nodes_key: str = 'nodes', print_opt: bool = False) -> Dict:

     # Validate arguments
    if {'nodes', 'links', 'directed'} <= set(ontology.keys()): 
         pass
    else: 
        raise TypeError(f'The given `ontology` is not a valid NetworkX JSON-graph object.')

    if model_nodes_key not in model.keys():
        raise KeyError(f'`{model_nodes_key}` is not a valid key of the `model` object.')

    if ontology_nodes_key not in ontology.keys():
        raise KeyError(f'`{ontology_nodes_key}` is not a valid key of the `ontology` object.')


    # Generate `nodeAtts` object
    nodeAtts = [{
        'node_id': node['id'],
        'db_ref_priority': None,
        'grounded_group': None,
        'type': None,
        'group_ids': None,
        'group_refs': None,
        'node_group_level': None
    } for node in model[model_nodes_key]]
    

    # Get list of IDs in ontology nodes
    ontology_ids = {node['id']: None for node in ontology[ontology_nodes_key]}

    
    # Check whether the priority grounding of each model node exists in the ontology
    for node, atts in zip(model[model_nodes_key], nodeAtts):
        
        # Completely ungrounded model nodes
        if len(node['db_ids']) < 1:
            atts['grounded_group'] = False
            atts['group_refs'] = ['not-grounded']
            atts['node_group_level'] = 1

        else:

            atts['db_ref_priority'] = f"{node['db_ids'][0]['namespace']}:{node['db_ids'][0]['id']}"


            # Model nodes that can be grounded to a node/group in the ontology
            if atts['db_ref_priority'] in ontology_ids.keys():
                atts['grounded_group'] = True
            

            # otherwise
            else:
                atts['grounded_group'] = False
                atts['group_refs'] = ['not-grounded']
                atts['node_group_level'] = 1
            

    # Output
    model['nodeAtts'] = nodeAtts


    return model


# %%
# Compute ancestry (shortest-to-ancestor ontology path) for each grounded model node
@task
def compute_onto_ancestry(model: List[Dict], ontology: Dict, model_nodes_key: str = 'nodes', model_nodeAtts_key: str = 'nodeAtts', print_opt: bool = False) -> Dict:

     # Validate arguments
    if {'nodes', 'links', 'directed'} <= set(ontology.keys()): 
         pass
    else: 
        raise TypeError(f'The given `ontology` is not a valid NetworkX JSON-graph object.')

    if model_nodes_key not in model.keys():
        raise KeyError(f'`{model_nodes_key}` is not a valid key of the `model` object.')

    if model_nodeAtts_key not in model.keys():
        raise KeyError(f'`{model_nodeAtts_key}` is not a valid key of the `model` object.')

    
    # Model nodes
    nodeAtts = model[model_nodeAtts_key]
    num_nodes = len(nodeAtts)


    # Load the ontology as a `NetworkX` object
    G = nx.readwrite.json_graph.node_link_graph(ontology)

    
    # Extract graph components
    comps = sorted(nx.weakly_connected_components(G), key = len, reverse = True)


    # Get the roots of each graph component
    comp_roots = [[node for node in comp if G.out_degree(node) < 1] for comp in comps]


    # Index of mappable model nodes
    node_indices = [i for i, node in enumerate(nodeAtts) if node['grounded_group']]


    # Index of the graph component to which the model nodes are mapped
    # (if in nontrivial subgraph -> -1)
    num_comps_nontrivial = sum([1 for comp in comps if len(comp) > 1])
    x = [{(nodeAtts[i]['db_ref_priority'] in comp): j for j, comp in enumerate(comps[:num_comps_nontrivial])} for i in node_indices]
    comp_indices = [d[True] if True in d.keys() else -1 for d in x]



    # Calculate shortest path to local ancestor/root
    for i, j in zip(node_indices, comp_indices):

        source = nodeAtts[i]['db_ref_priority']

        # Case: model node was mapped to either a trivial component or the root of a non-trivial component
        if (j == -1) or (source in comp_roots[j]):

            nodeAtts[i]['node_group_level'] = 1
            nodeAtts[i]['group_refs'] = [source]

        else:

            z = []
            for target in comp_roots[j]:

                try:
                    p = nx.algorithms.shortest_paths.generic.shortest_path(G.subgraph(comps[j]), source = source, target = target)
                    z.append(p)

                except:
                    pass
            

            if len(z) > 0:

                # Find shortest path
                z = sorted(z, key = len, reverse = False)
                
                
                # Group level of the model node 
                # = Level of its ontology group to which it is grounded + 1 
                # = length of the shortest path to the ancestor group
                nodeAtts[i]['node_group_level'] = len(z[0])


                # Reverse path such that [target, ..., source]
                nodeAtts[i]['group_refs'] = z[0][::-1]

            else:
                
                nodeAtts[i]['node_group_level'] = 1
                nodeAtts[i]['group_refs'] = [source]


    # Ensure that identical ontology nodes share the same lineage (path to their ancestor) for hierarchical uniqueness
    group_refs = [node['group_refs'] for node in nodeAtts]
    m = max([len(path) for path in group_refs])
    for i in range(1, m):

        # All nodes
        x = [path[i] if len(path) > i else '' for path in group_refs]


        # All unique nodes
        y = list(set(x) - set(['']))


        # Mapping from all nodes to unique nodes
        xy = [y.index(node) if node != '' else '' for node in x]


        # Choose the path segment of the first matching node for each unique node
        z = [group_refs[x.index(node)][:i] for node in y]
        

        # Substitute path segments
        for j in range(num_nodes):

            if xy[j] != '':
                nodeAtts[j]['group_refs'][:i] = z[xy[j]]

            else:
                nodeAtts[j]['group_refs'][:i] = group_refs[j][:i]


    # Model node type = ontology ID or `ref` of the group that is the ancestor of the group to which the model node is grounded
    for node in nodeAtts:
        node['type'] = node['group_refs'][0]


    # Output
    model['nodeAtts'] = nodeAtts


    # Message
    if print_opt == True:

        logger = prefect.context.get('logger')
        logger.info(f"Ontology: {len(comps)} connected components, {num_comps_nontrivial / len(comps) * 100:.2f} % are non-trivial")


    return model


# %%
# Generate `groups` from the ontology ancestry of all model nodes
@task
def generate_onto_groups(model: List[Dict], ontology: Dict, model_nodeAtts_key: str = 'nodeAtts', print_opt: bool = False) -> Dict:


    # Node attributes
    nodeAtts = model[model_nodeAtts_key]


    # Generate list of mapped ontology categories, sorted by size
    group_refs = [node['group_refs'] for node in nodeAtts]
    groups_ = {}
    groups_['ref'], groups_['size'] = np.unique([node for path in group_refs for node in path], return_counts = True)

    num_groups = len(groups_['ref'])
    i = np.argsort(groups_['size'])[::-1]
    groups_['ref'] = list(groups_['ref'][i])
    groups_['size'] = [int(k) for k in groups_['size']]
    groups_['id'] = list(range(num_groups))
    groups_['id_onto'] = groups_['ref']


    # Transform local IDs to global IDs
    groups_['id'] = [transform_obj_ids(obj = id, obj_type = 'groups') for id in groups_['id']]


    # Load the ontology graph as a `networkx` object
    G = nx.readwrite.json_graph.node_link_graph(ontology)


    # Get the mapped ontology node/group names
    x = dict(G.nodes(data = 'name', default = None))
    groups_['name'] = list(np.empty((num_groups, )))
    for i, ref in enumerate(groups_['ref']):

        if ref in x.keys():

            if x[ref] == None:
                groups_['name'][i] = ref

            elif len(x[ref]) == 0:
                groups_['name'][i] = ref
            
            else:
                groups_['name'][i] = x[ref]

        else:
            groups_['name'][i] = ref


    # Get onto level of each group
    i = max([len(path) for path in group_refs])
    x = [np.unique([path[j] if len(path) > j else '' for path in group_refs]) for j in range(i)]
    groups_['level'] = [int(np.flatnonzero([ref in y for y in x])[0]) for ref in groups_['ref']]


    # Get numeric ID version of ontocat_refs
    x = {k: v for k, v in zip(groups_['ref'], groups_['id'])}
    group_ids = [[x[node] for node in path] for path in group_refs]
    for node in nodeAtts:
        node['group_ids'] = [x[group] for group in node['group_refs']]


    # Get parent group ID for each group (for ancestor nodes, parentID = None)
    y = [np.flatnonzero([True if ref in path else False for path in group_refs])[0] for ref in groups_['ref']]
    groups_['parent_ref'] = [group_refs[y[i]][group_refs[y[i]].index(ref) - 1] if group_refs[y[i]].index(ref) > 0 else None for i, ref in enumerate(groups_['ref'])]
    groups_['parent_id'] = [x[parent] if parent is not None else None for parent in groups_['parent_ref']]


    # Get ID of all children groups
    map_selves_children = {group_id: [] for group_id in groups_['id']}
    for self_id, parent_id in zip(groups_['id'], groups_['parent_id']):
        if parent_id != None:
            map_selves_children[parent_id].append(self_id)

    groups_['children_ids'] = [map_selves_children[group_id] for group_id in groups_['id']]


    # Find membership of onto group (all model nodes that have been grounded to this group and any of its children)
    groups_['node_ids_all'] = [[node['node_id'] for node, path in zip(nodeAtts, group_refs) if ref in path] for ref in groups_['ref']]


    # Find direct membership of each group
    groups_['node_ids_direct'] = [[node['node_id'] for node, path in zip(nodeAtts, group_refs) if ref == path[-1]] for ref in groups_['ref']]


    # Model ID
    groups_['model_id'] = [model['id'] for i in range(num_groups)]


    # Switch to row-wise structure
    model['groups'] = [{k: groups_[k][i] for k in groups_.keys()} for i in range(num_groups)]
    model[model_nodeAtts_key] = nodeAtts

    # Message
    if print_opt == True:

        logger = prefect.context.get('logger')
        logger.info(f"Model {model['id_emmaa']}: {num_groups} groups, levels = {min(groups_['level'])} - {max(groups_['level'])}")


    return model

