import json
import logging
import os
import pathlib
import shutil
import subprocess
import sys
import traceback
import requests
import yaml
from datetime import datetime

from aced_submission.meta_flat_load import DEFAULT_ELASTIC, load_flat
from aced_submission.meta_flat_load import delete as meta_flat_delete
from aced_submission.grip_load import bulk_load, get_project_data, \
    delete_project as grip_delete
from opensearchpy import OpenSearch as Elasticsearch
from opensearchpy import OpenSearchException
from gen3.auth import Gen3Auth
from gen3.file import Gen3File
from gen3_tracker.config import Config
from gen3_tracker.git.snapshotter import push_snapshot
from gen3_tracker.meta.dataframer import LocalFHIRDatabase

logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def _get_grip_service() -> str | None:
    """Get GRIP_SERVICE_NAME from environment"""
    return os.environ.get('GRIP_SERVICE_NAME', None)


def _get_token() -> str | None:
    """Get ACCESS_TOKEN from environment"""
    return os.environ.get('ACCESS_TOKEN', None)


def _auth(access_token) -> Gen3Auth:
    """Authenticate using ACCESS_TOKEN"""
    if access_token:
        # use access token from environment (set by sower)
        return Gen3Auth(refresh_file=f"accesstoken:///{access_token}")
    # no access token, use refresh token set in default ~/.gen3/credentials.json location
    return Gen3Auth()


def _user(auth: Gen3Auth) -> dict:
    """Get user info from arborist"""
    return auth.curl('/user/user').json()


def _input_data() -> dict:
    """Get input data"""
    assert 'INPUT_DATA' in os.environ, "INPUT_DATA not found in environment"
    return json.loads(os.environ['INPUT_DATA'])


def _get_program_project(input_data: dict) -> tuple:
    """Get program and project from input_data"""
    assert 'project_id' in input_data, "project_id not found in INPUT_DATA"
    assert '-' in input_data['project_id'], 'project_id must be in the format <program>-<project>'
    return input_data['project_id'].split('-')


def _can_create(output: dict,
                program: str,
                project: str,
                user: dict) -> bool:
    """Check if user can create a project in the given program.

    Args:
        output: output dict the json that will be returned to the caller
        program: program Gen3 program(-project)
        project: project Gen3 (program-)project
        user: user dict from arborist (aka profile)
    """

    can_create = True

    required_resources = [
        f"/programs/{program}",
        f"/programs/{program}/projects"
    ]
    for required_resource in required_resources:
        if required_resource not in user['resources']:
            output['logs'].append(f"{required_resource} not found in user resources")
            can_create = False
        else:
            output['logs'].append(f"HAS RESOURCE {required_resource}")

    required_services = [
        f"/programs/{program}/projects/{project}"
    ]
    for required_service in required_services:
        if required_service not in user['authz']:
            output['logs'].append(f"{required_service} not found in user authz")
            can_create = False
        else:
            if {'method': 'create', 'service': '*'} not in user['authz'][required_service]:
                output['logs'].append(f"create not found in user authz for {required_service}")
                can_create = False
            else:
                output['logs'].append(f"HAS SERVICE create on resource {required_service}")

    return can_create


def _can_read(output: dict,
              program: str,
              project: str,
              user: dict) -> bool:
    """Check if user can read a project in the given program.

    Args:
        output: output dict the json that will be returned to the caller
        program: program Gen3 program(-project)
        project: project Gen3 (program-)project
        user: user dict from arborist (aka profile)
    """

    can_read = True

    required_resources = [
        f"/programs/{program}",
        f"/programs/{program}/projects"
    ]
    for required_resource in required_resources:
        if required_resource not in user['resources']:
            output['logs'].append(f"{required_resource} not found in user resources")
            can_read = False
        else:
            output['logs'].append(f"HAS RESOURCE {required_resource}")

    required_services = [
        f"/programs/{program}/projects/{project}"
    ]
    for required_service in required_services:
        if required_service not in user['authz']:
            output['logs'].append(f"{required_service} not found in user authz")
            can_read = False
        else:
            if {'method': 'read-storage', 'service': '*'} not in user['authz'][required_service]:
                output['logs'].append(f"read-storage not found in user authz for {required_service}")
                can_read = False
            else:
                output['logs'].append(f"HAS SERVICE read-storage on resource {required_service}")

    return can_read


def _download_and_unzip(object_id: str,
                        file_path: str,
                        output: dict,
                        file_name: str) -> bool:
    """Download and unzip object_id to downloads/{file_path}"""
    try:
        token = _get_token()
        auth = _auth(token)
        file_client = Gen3File(auth)
        full_download_path = (pathlib.Path('downloads') / file_name)
        full_download_path_parent = full_download_path.parent
        full_download_path_parent.mkdir(parents=True, exist_ok=True)
        file_client.download_single(object_id, 'downloads' )
    except Exception as e:
        output['logs'].append(f"An Exception Occurred: {str(e)}")
        output['logs'].append(f"ERROR DOWNLOADING {object_id} {file_path}")
        raise e
        return False

    output['logs'].append(f"DOWNLOADED {object_id} {file_path}")

    cmd = f"unzip -o -j {full_download_path} -d {file_path}".split()
    result = subprocess.run(cmd)
    if result.returncode != 0:
        output['logs'].append(f"ERROR UNZIPPING /tmp/{object_id}")
        if result.stderr:
            output['logs'].append(result.stderr.read().decode())
        if result.stdout:
            output['logs'].append(result.stdout.read().decode())
        return False

    output['logs'].append(f"UNZIPPED {file_path}")
    return True


def _load_all(study: str,
              project_id: str,
              output: dict,
              file_path: str,
              schema: str,
              work_path: str) -> bool:

    if study is None or study == "":
        output['logs'].append("Please provide a study name")
        return False

    if project_id is None or project_id == "":
        output['logs'].append("Please provide a project_id (program-project)")
        return False

    logs = None
    try:
        program, project = project_id.split('-')
        assert program, output['logs'].append("program is required")
        assert project, output['logs'].append("project is required")

        file_path = pathlib.Path(file_path)
        extraction_path = file_path / 'extractions'
        research_study = str(extraction_path / 'ResearchStudy.ndjson')

        file_path = str(file_path)
        extraction_path = str(extraction_path)
        output['logs'].append(f"Simplifying study: {file_path}")

        # call jsonschemagraph to create edges and vertices
        graph_gen_cmd = ["jsonschemagraph", "gen-dir", "iceberg/schemas/graph", f"{file_path}", f"{extraction_path}","--project_id", f"{project_id}","--gzip_files"]
        subprocess.run(graph_gen_cmd, check=True, capture_output=True)

        bulk_load(_get_grip_service(), "CALIPER",f"{program}-{project}", extraction_path, output, _get_token())

        assert pathlib.Path(work_path).exists(), f"Directory {work_path} does not exist."
        work_path = pathlib.Path(work_path)
        db_path = (work_path / "local_fhir.db")
        db_path.unlink(missing_ok=True)

        db = LocalFHIRDatabase(db_name=db_path)
        db.bulk_insert_data(resources=get_project_data(_get_grip_service(), "CALIPER", f"{program}-{project}", output, _get_token()))

        index_generator_dict = {
            'researchsubject': db.flattened_research_subjects,
            'specimen': db.flattened_specimens,
            'file': db.flattened_document_references
        }

        # To ensure differences in the dataframer versions do not conflict, clear the project, and reload the project.
        for index in index_generator_dict.keys():
            meta_flat_delete(project_id=f"{program}-{project}", index=index)

        for index, generator in index_generator_dict.items():
            load_flat(project_id=project_id, index=index,
                    generator=generator(),
                    limit=None, elastic_url=DEFAULT_ELASTIC,
                    output_path=None)

    # when generating graph with jsonschemagraph
    except subprocess.CalledProcessError as exception:
        # save and print any useful logs
        tb = traceback.print_tb(exception.__traceback__)
        for title, log in [("stdout", exception.stdout), ("traceback", tb), ("ERROR", exception.stderr)]:
            if not log:
                continue
            
            message = f"{title.upper()}: {log}"
            output['logs'].append(message)
            print(message)

        # print final error
        final_error = f"ERROR: Unable to generate valid jsonschema graph from {file_path} to {extraction_path} for project ID {project_id}"
        # exception.stdout.append(f"\n{final_error}")
        output['logs'].append(final_error)
        print(f"[out] {json.dumps(output, separators=(',', ':'))}")
        raise

    # when making changes to Elasticsearch
    except OpenSearchException as e:
        output['logs'].append(f"An ElasticSearch Exception occurred: {str(e)}")
        tb = traceback.format_exc()
        print("TRACEBACK: ", tb)
        print("OpenSearchException: ", str(e))
        output['logs'].append(tb)
        if logs is not None:
            output['logs'].extend(logs)
        return False

    # all other exceptions
    except Exception as e:
        output['logs'].append(f"An Exception Occurred: {str(e)}")
        tb = traceback.format_exc()
        print("TRACEBACK: ", tb)
        print("Exception: ", str(e))
        output['logs'].append(tb)
        if logs is not None:
            output['logs'].extend(logs)
        return False

    output['logs'].append(f"Loaded {study}")
    if logs is not None:
        output['logs'].extend(logs)
    return True


def _empty_project(output: dict,
                   program: str,
                   project: str,
                   user: dict,
                   dictionary_path: str | None = None,
                   config_path: str | None = None):
    """Clear out graph and flat metadata for project """
    # check permissions
    try:
        grip_delete(_get_grip_service(), graph_name="CALIPER", project_id=f"{program}-{project}",
                    output=output, access_token=_get_token())
        output['logs'].append(f"EMPTIED graph for {program}-{project}")

        for index in ["researchsubject", "specimen", "file"]:
            meta_flat_delete(project_id=f"{program}-{project}", index=index)
        output['logs'].append(f"EMPTIED flat for {program}-{project}")

    except Exception as e:
        output['logs'].append(f"An Exception Occurred emptying project {program}-{project}: {str(e)}")
        tb = traceback.format_exc()
        output['logs'].append(tb)


def main():
    token = _get_token()
    auth = _auth(token)

    print("[out] authorized successfully")
    print("[out] retrieving user info...")
    user = _user(auth)

    output = {'user': user['email'], 'files': [], 'logs': []}
    # note, only the last output (a line in stdout with `[out]` prefix) is returned to the caller

    # output['env'] = {k: v for k, v in os.environ.items()}
    
    input_data = _input_data()
    print(f"[out] {json.dumps(input_data, separators=(',', ':'))}")
    program, project = _get_program_project(input_data)

    schema = os.getenv('DICTIONARY_URL', None)

    if schema is None:
        schema = 'https://aced-public.s3.us-west-2.amazonaws.com/aced-test.json'
        output['logs'].append(f"DICTIONARY_URL not found in environment using {schema}")

    method = input_data.get("method", None)
    assert method, "input data must contain a `method`"

    if method.lower() == 'put':
        # read from bucket, write to fhir store
        _put(input_data, output, program, project, user, schema)
    elif method.lower() == 'delete':
        _empty_project(output, program, project, user, dictionary_path=schema,
                    config_path="config.yaml")
    else:
        raise Exception(f"unknown method {method}")


    # note, only the last output (a line in stdout with `[out]` prefix) is returned to the caller
    print(f"[out] {json.dumps(output, separators=(',', ':'))}")


def _put(input_data: dict,
         output: dict,
         program: str,
         project: str,
         user: dict,
         schema: str):
    """Import data from bucket to graph, flat and fhir store."""
    # check permissions
    can_create = _can_create(output, program, project, user)
    output['logs'].append(f"CAN CREATE: {can_create}")
    if not can_create:
        raise Exception(f"401: No permissions to create project {project} on program {program}. \nYou can view your project-level permissions with g3t ping")
    assert 'push' in input_data, "input data must contain a `push`"
    for commit in input_data['push']['commits']:
        assert 'object_id' in commit, "commit must contain an `object_id`"
        object_id = commit['object_id']
        assert object_id, "object_id must not be empty"
        assert 'commit_id' in commit, "commit must contain a `commit_id`"
        commit_id = commit['commit_id']
        assert commit_id, "commit_id must not be empty"
        file_path = f"/root/studies/{project}/commits/{commit_id}"
        pathlib.Path(file_path).mkdir(parents=True, exist_ok=True)
        # get the meta data file
        if _download_and_unzip(object_id, file_path, output, commit['meta_path']):

            # tell user what files were found
            for _ in pathlib.Path(file_path).glob('*'):
                output['files'].append(str(_))

            # load the study into the database and elastic search
            _load_all(project, f"{program}-{project}", output, file_path, schema, "work")
        
        shutil.rmtree(f"/root/studies/{project}")


if __name__ == '__main__':
    main()
