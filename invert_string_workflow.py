import logging
from pprint import pprint
import typing

import orquestra.sdk as orq


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
    ],
    data_aggregation=orq.DataAggregationDefinition(
        run=True,
        resource_def=orq.ResourceDefinition(
            cpu="500m",
            memory="500Mi",
            disk="100Mi",
        ),
    )
)
def invert_string_workflow(strings_to_invert: typing.List[str]) -> typing.List[orq.TaskDefinition]:
    """
    A workflow that takes as input a list of strings that the workflow should invert.
    :param strings_to_invert:
    :return:
    """
    return [invert_string(str_) for str_ in strings_to_invert]


workflow: orq.WorkflowDefinition = invert_string_workflow(["the-santa-clause", "palindrome", "abba"])

workflow.validate()
pprint(workflow.to_json())


