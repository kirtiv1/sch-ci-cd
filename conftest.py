import logging

import pytest

logger = logging.getLogger('streamsets.ci_cd_poc')


def pytest_addoption(parser):
    parser.addoption('--pipeline-id')
    parser.addoption('--upgrade-jobs', action='store_true')


@pytest.fixture(scope='session')
def sch(sch):
    yield sch


@pytest.fixture(scope='session')
def pipeline(sch, request):
    pipeline_id = request.config.getoption('pipeline_id')
    pipeline_ = sch.pipelines.get(pipeline_id=pipeline_id)

    yield pipeline_

    # Remove the job that was created from integration tests included in test_tdf_data_to_elasticsearch.py
    jobs_to_delete = sch.jobs.get_all(pipeline_id=pipeline_.pipeline_id, description='DataOps CI/CD test job')
    if jobs_to_delete:
        logger.debug('Deleting test jobs: %s ...', ', '.join(str(job) for job in jobs_to_delete))
        sch.delete_job(*jobs_to_delete)

    # If the tests passed and upgrade_jobs is requested, then upgrade the job/s
    # with the latest version of the pipeline
    if not request.session.testsfailed:
        if request.config.getoption('upgrade_jobs'):
            pipeline_ = sch.pipelines.get(pipeline_id=pipeline_id)
            jobs_to_upgrade = [job for job in sch.jobs.get_all(pipeline_id=pipeline_id)
                               if job.pipeline_commit_label != f'v{pipeline_.version}']
            if jobs_to_upgrade:
                logger.info('Upgrading jobs: %s ...', ', '.join(str(job) for job in jobs_to_upgrade))
                sch.upgrade_job(*jobs_to_upgrade)
            else:
                logger.warning('No jobs need to be upgraded')
