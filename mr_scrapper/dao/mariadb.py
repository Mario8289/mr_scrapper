from pandas import DataFrame
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table


class MariaDbDao:
    def __init__(self, engine):
        self.engine = engine
        self.row_cnt: int = 0

    def commit(self, table: str, df: DataFrame, replace_values: bool =False):
        self._with_session(table, df, replace_values)

    def _execute(self, session, table, df, replace_values):

        meta = MetaData()

        table_meta = Table(table, meta, autoload=True, autoload_with=self.engine)

        update_columns = [x.name for x in table_meta.columns._all_columns if not x.primary_key and x.name in df.columns]
        values = [tuple([x2 if x2 is not None else "NULL" for x2 in x]) for x in df.values]
        query = f"INSERT INTO {table} ({', '.join(map(str, df.columns))}) VALUES {', '.join(map(str, values))}"

        if replace_values:
            # on duplicate key replace the update columns with the incoming values
            update_query = ", ".join(
                map(str, [f"{x} = VALUES({x})" for x in update_columns])
            )
            query = f"{query} ON DUPLICATE KEY UPDATE {update_query}"

        session.execute(query)

        print(f"ROWS: {df.shape[0]}, WRITTEN TO TABLE: {table}")

    def _with_session(self, table: str, df: DataFrame, replace_values: bool):
        session_maker = sessionmaker(bind=self.engine)
        session = session_maker()
        try:
            rows_per_insert = 100

            for start in range(0, df.shape[0], rows_per_insert):
                df_subset = df.iloc[start : start + rows_per_insert]
                self._execute(session, table, df_subset, replace_values)

            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
