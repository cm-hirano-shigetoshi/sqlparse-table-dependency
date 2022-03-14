import sqlparse
from sqlparse import sql as C
from sqlparse import tokens as T


def collect_source_tables(statement):
    return _collect_source_tables_recursively(statement)


def get_with_identifier_dict(statement):
    def get_with_content(idf):
        _, paren = idf.token_next_by(i=C.TokenList)
        return paren

    idx, _ = statement.token_next_by(m=(T.Keyword.CTE, "with"))
    if not idx:
        return {}
    _, idf_ls = statement.token_next_by(i=C.TokenList, idx=idx)
    if isinstance(idf_ls, C.IdentifierList):
        d = {}
        for idf in idf_ls.get_sublists():
            d[idf.get_real_name()] = get_with_content(idf)
        return d
    else:
        idf = idf_ls
        return {idf.get_real_name(): get_with_content(idf)}


def get_insert_into_set(statement):
    if statement.get_type() != "INSERT":
        return set()

    def get_real_name_of_insert(idf):
        idx, _ = idf.token_next_by(t=T.Whitespace)
        return "".join([t.value for t in idf.tokens[:idx]])

    idx, _ = statement.token_next_by(m=(T.Keyword.DML, "insert"))
    _, idf_ls = statement.token_next_by(i=C.TokenList, idx=idx)
    if isinstance(idf_ls, C.IdentifierList):
        return {get_real_name_of_insert(idf) for idf in idf_ls.get_sublists()}
    else:
        idf = idf_ls
        return {get_real_name_of_insert(idf)}


def get_from_identifier_list(statement, idx=-1):
    idx, _ = statement.token_next_by(m=(T.Keyword, "from"), idx=idx)
    if not idx:
        return []
    _, idf_ls = statement.token_next_by(i=C.TokenList, idx=idx)
    if isinstance(idf_ls, C.IdentifierList):
        return [idf for idf in idf_ls.get_sublists()]
    else:
        return [idf_ls]


def get_join_idfs(statement, idx=-1):
    idx, _ = statement.token_next_by(m=(T.Keyword, "join$", True), idx=idx)
    if not idx:
        return []
    _, idf_ls = statement.token_next_by(i=C.TokenList, idx=idx)
    if isinstance(idf_ls, C.IdentifierList):
        return [idf for idf in idf_ls.get_sublists()]
    else:
        return [idf_ls]


def _collect_source_tables_recursively(statement):
    withs = _create_withs(statement)
    source_tables = _collect_source_tables_local(statement)
    # print(f"with = {withs}")
    # print(f"source_tables = {source_tables}")
    return _expand_with(source_tables, withs)


def _create_withs(statement):
    with2idfs = get_with_identifier_dict(statement)
    if len(with2idfs) == 0:
        return {}
    return {k: collect_source_tables(v) for k, v in with2idfs.items()}


def _get_insert_dependency(statement):
    insert_into_set = get_insert_into_set(statement)
    source_tables = collect_source_tables(statement)
    for insert_into in insert_into_set:
        print(f"{insert_into}\n  <= {source_tables}")


def _collect_source_tables_local(statement):
    def get_identifier_content(idf):
        return idf.token_first()

    def is_subquery(idf):
        return isinstance(get_identifier_content(idf), C.Parenthesis)

    def get_real_name_of_from(idf):
        idx, _ = idf.token_next_by(t=T.Whitespace)
        return "".join([t.value for t in idf.tokens[:idx]])

    source_set = set()
    for idf in get_from_identifier_list(statement):
        if is_subquery(idf):
            source_set |= collect_source_tables(get_identifier_content(idf))
        else:
            source_set.add(get_real_name_of_from(idf))

    for idf in get_join_idfs(statement):
        if is_subquery(idf):
            source_set |= collect_source_tables(get_identifier_content(idf))
        else:
            source_set.add(get_real_name_of_from(idf))
    return source_set


def _expand_with(names, withs):
    while True:
        found = False
        new = set()
        # print(f"#{names}")
        for name in names:
            if name in withs:
                found = True
                new |= withs[name]
            else:
                new.add(name)
        names = new
        if not found:
            break
    return names
