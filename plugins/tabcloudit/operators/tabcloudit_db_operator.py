from airflow.utils.decorators import apply_defaults
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy.orm import sessionmaker, Session

def get_session(conn_id: str) -> Session:
    db_hook = MySqlHook(mysql_conn_id=conn_id)
    engine = db_hook.get_sqlalchemy_engine()
    return sessionmaker(bind=engine)()

class TabclouditDbOperator(PythonOperator):
    @apply_defaults
    def __init__(self, conn_id: str, *args, **kwargs) -> None:
        self.conn_id = conn_id
        self.args = args
        self.kwargs = kwargs
        super().__init__(*args, **kwargs)

    def execute_callable(self) -> None:
        session = get_session(self.conn_id)
        try:
            result = self.python_callable(*self.args, session=session, **self.kwargs)
        except Exception:
            session.rollback()
            raise
        session.commit()
        return result
