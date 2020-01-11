from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults


class TestdummyOperator(BaseOperator):
    """
    just testing pluggins
    """
    ui_color = '#e8f7e4'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(TestdummyOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        pass
