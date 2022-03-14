import sqlparse

import statement_helper as s


def test_insert_into():
    sql = """
        insert into schema.phys_t_1
        with aaa as (
          select * from schema.phys_t_2
        )
        select * from aaa, schema.phys_t_3
        join schema.phys_t_4
        on aaa.x = schema.phys_t_4.x
    """

    statements = sqlparse.parse(sql)
    statement = statements[0]
    ans = s.get_insert_into_set(statement)
    assert ans == {"schema.phys_t_1"}
    ans = s.collect_source_tables(statement)
    assert ans == {"schema.phys_t_2", "schema.phys_t_3", "schema.phys_t_4"}
    ans = s.get_with_identifier_dict(statement)
    assert isinstance(ans["aaa"], sqlparse.sql.Parenthesis)
    assert ans["aaa"].value[1:-1].strip(), "select * from schema.phys_t_2"
    ans = s.get_from_identifier_list(statement)
    assert ans[0].value == "aaa"
    assert ans[1].value == "schema.phys_t_3"
    ans = s.get_join_idfs(statement)
    assert ans[0].value == "schema.phys_t_4"


if __name__ == "__main__":
    test_insert_into()
