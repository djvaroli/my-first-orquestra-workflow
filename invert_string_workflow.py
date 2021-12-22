import logging
import json
import typing

import orquestra.sdk as orq
from orquestra.runtime import RemoteRuntime


@orq.task(
    resource_def=orq.ResourceDefinition(
        cpu="1000m",
        memory="1000Mi",
        disk="1Gi"
    ),
)
def invert_string(string_: str) -> str:
    """
    Given a string returns its inverse
    :param string_:
    :return:
    """
    logging.debug("Reporting from the 'invert_string' function!")
    return string_[::-1]


@orq.workflow(
    name="invert-a-string-workflow",
    import_defs=[
        orq.GitImportDefinition.get_current_repo_and_branch()
    ]
)
def invert_string_workflow(strings_to_invert: typing.List[str]) -> typing.List[orq.TaskDefinition]:
    """
    A workflow that takes as input a list of strings that the workflow should invert.
    :param strings_to_invert:
    :return:
    """
    return [invert_string(str_) for str_ in strings_to_invert]


workflow: orq.WorkflowDefinition = invert_string_workflow(["the-santa-clause", "palindrome", "abba"])

# validate the workflow
workflow.validate()

# save the config to a json file
workflow_config = workflow.to_json()
workflow_name = workflow_config["name"]
with open(f"{workflow_name}_config.json", "w+") as f:
    json.dump(workflow_config, f)


# submit to Orquestra
runtime = RemoteRuntime()
workflow_id = runtime.create_workflow(workflow)

print("CREATE WORKFLOW RESP", workflow_id)
print("STATUS WORKFLOW RESP", runtime.get_workflow_status(workflow_id))
print("GET WORKFLOW DETAILS", runtime.get_workflow(workflow_id))
print("GET WORKFLOW RESULT", runtime.get_workflow_result(workflow_id))
print("LIST WORKFLOW ARTIFACTS", runtime.list_artifacts(workflow_id))
print("STOP WORKFLOW RESP", runtime.stop_workflow(workflow_id))
print("RETRY WORKFLOW RESP", runtime.retry_workflow(workflow_id))
print("LIST WORKFLOWS", runtime.list_workflows())
