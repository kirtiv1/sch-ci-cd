import logging

logger = logging.getLogger(__name__)

TEST_DATA = [dict(id=1, year=2422, rank=1, name='JEAN-LUC PICARD', number=1, team='TDF 2422',
                  time='94h 33m 14s', hours=94, mins=33, secs=14),
             dict(id=2, year=2422, rank=2, name='WILLIAM RIKER', number=37, team='TDF 2422',
                  time='97h 32m 35s', hours=97, mins=32, secs=35),
             dict(id=3, year=2422, rank=3, name='BEVERLY CRUSHER', number=39, team='TDF 2422',
                  time='99h 02m 38s', hours=99, mins=2, secs=38)]


def test_complete(sch, pipeline):
    """Test that whole output is received exactly as expected."""
    EXPECTED_RECORDS = [dict(year=2422, rank=1, number=1, team='TDF 2422', time='94h 33m 14s', hours=94,
                             mins=33, secs=14, firstName='Jean-Luc', lastName='Picard'),
                        dict(year=2422, rank=2, number=37, team='TDF 2422', time='97h 32m 35s',
                             hours=97, mins=32, secs=35, firstName='William', lastName='Riker'),
                        dict(year=2422, rank=3, number=39, team='TDF 2422', time='99h 02m 38s', hours=99,
                             mins=2, secs=38, firstName='Beverly', lastName='Crusher')]

    preview = sch.run_pipeline_preview(pipeline, test_data=TEST_DATA).preview
    actual_output = [item.field for item in preview['JythonEvaluator_01'].output]
    assert actual_output == EXPECTED_RECORDS


def test_remove_id_field(sch, pipeline):
    """Test that id field is removed as expected."""
    preview = sch.run_pipeline_preview(pipeline, test_data=TEST_DATA).preview
    actual_output = [item.field for item in preview['JythonEvaluator_01'].output]
    assert all('id' not in record for record in actual_output)


def test_split_name(sch, pipeline):
    """Test that first name and last name are split as expected."""
    EXPECTED_NAMES = [dict(firstName='Jean-Luc', lastName='Picard'),
                      dict(firstName='William', lastName='Riker'),
                      dict(firstName='Beverly', lastName='Crusher')]

    preview = sch.run_pipeline_preview(pipeline, test_data=TEST_DATA).preview
    actual_output = [item.field for item in preview['JythonEvaluator_01'].output]
    assert EXPECTED_NAMES == [{key: record[key] for key in ['firstName', 'lastName']}
                              for record in actual_output]
