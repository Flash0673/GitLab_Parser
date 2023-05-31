import pandas as pd
from Parser import Parser


def test_get_project_commits_with_basic_features_web():
    p = Parser(token="YOUR_TOKEN")
    p_1 = Parser()
    df_1 = p.get_project_commits_with_basic_features_web(project_id=11163)
    df_2 = p_1.get_project_commits_with_basic_features_web(project_id=11163)
    assert list(df_1.columns) == ['project_id', 'commit_id', 'commit_message', 'files_changed',
                                  'lines_inserted', 'lines_deleted', 'diff', 'target'], "Не все колонки"
    assert df_2 is None, "Не тот return"


def test_get_merge_commits_web():
    p = Parser(token="YOUR_TOKEN")
    df_1 = p.get_merge_commits_web(project_id=1426, iid=1)
    assert list(df_1.columns) == ['project_id', 'commit_id', 'commit_message', 'files_changed', 'lines_inserted', 'lines_deleted', 'diff', 'target',
     'changes', 'add', 'del', 'web_url', 'files changed', 'Imports added', 'Imports deleted', 'path', 'file_new',
     'file_past']
    df_2 = p.get_merge_commits_web(project_id=1425, iid=1)
    assert df_2 is None

