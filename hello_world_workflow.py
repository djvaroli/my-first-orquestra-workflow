# This example shows how to define a workflow using the Orquestra SDK. For examples of
# how to execute a workflow (locally or remotely), see the orquestra-runtime repository.

import logging
from pprint import pprint
import typing

import orquestra.sdk as orq


@orq.task(
    resource_def=orq.ResourceDefinition(
        cpu="1000m",
        memory="1000Mi",
        disk="1Gi",
    ),
)
def get_name(title: str, first_name: str, last_name: str) -> str:
    logging.debug("Debug message from get_name")
    return f"{title}. {first_name} {last_name}"


@orq.task(
    resource_def=orq.ResourceDefinition(
        cpu="1000m",
        memory="1000Mi",
        disk="1Gi",
    ),
)
def get_greeting(name: str) -> str:
    logging.warning("warning message from get_greeting")
    print("print message from get_greeting")
    return f"Hello {name}!!"


@orq.workflow(
    name="hello-pipeline-{first_name}-{last_name}",
    import_defs=[
        orq.GitImportDefinition.get_current_repo_and_branch(),
    ],
    data_aggregation=orq.DataAggregationDefinition(
        run=True,
        resource_def=orq.ResourceDefinition(
            cpu="500m",
            memory="500Mi",
            disk="100Mi",
        ),
    ),
)
def hello_workflow(
    first_name: str, last_name: str = "More", greeting: str = "Mr"
) -> typing.List[orq.TaskDefinition]:
    return [
        get_greeting(get_name(greeting, first_name, last_name)) for _ in [1, 2, 3, 4, 5]
    ]


if __name__ == '__main__':
    from orquestra.runtime import RemoteRuntime

    # top level "workflow" object used by ORQ CLI to submit workflow
    workflow: orq.WorkflowDefinition = hello_workflow("Daniel")

    # validation
    workflow.validate()

    pprint(workflow.to_json())

    runtime = RemoteRuntime()

    workflow_id = runtime.create_workflow(workflow)
    print(workflow_id)

