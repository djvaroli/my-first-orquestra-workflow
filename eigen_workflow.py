import logging
from pprint import pprint
import typing

import numpy as np
import orquestra.sdk as orq


@orq.task(
    resource_def=orq.ResourceDefinition(
        cpu="1000m",
        memory="1000Mi",
        disk="1Gi"
    ),
)
def generate_random_square_matrix(size: int = 4) -> np.matrix:
    """
    Generates and returns a random square matrix of a specified size.
    :param size:
    :return:
    """
    return np.random.random((size, size))


@orq.task(
    resource_def=orq.ResourceDefinition(
        cpu="1000m",
        memory="1000Mi",
        disk="1Gi"
    ),
)
def find_eigenvalues_and_eigen_vectors(matrix: np.matrix) -> typing.Tuple:
    """
    Given a square matrix, finds the Eigenvalues and Eigenvectors
    :param matrix:
    :return:
    """
    return np.linalg.eig(matrix)



@orq.workflow(
    name="test-eigenvectors-and-eigenvalues-workflow",
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
def eigen_workflow(matrix_sizes: typing.List[int]) -> typing.List[orq.TaskDefinition]:
    """
    A workflow that takes as input a list of integers which define the size of the square matrix that it is to generate
    and then find the eigenvalues and eigenvectors of.
    :param matrix_sizes:
    :return:
    """
    return [
        find_eigenvalues_and_eigen_vectors(generate_random_square_matrix(m_size_)) for m_size_ in matrix_sizes
    ]

workflow: orq.WorkflowDefinition = eigen_workflow([3, 4, 5])

workflow.validate()
pprint(workflow.to_json())


