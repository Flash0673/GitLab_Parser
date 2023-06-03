import pandas as pd
from pathlib import Path

from Parser import Parser


def test_get_merge_requests_file():
    p = Parser(token="BxntftQ1zwq_28vtS2Qm")
    p.get_merge_requests_file(input_file="Data/test.json",
                              output_file="Data/get_merge_requests_file/test_output_get_merge_requests_file.json")
    with open("Data/get_merge_requests_file/output_get_merge_requests_file.json") as f:
        correct = f.readlines()
    with open("Data/get_merge_requests_file/test_output_get_merge_requests_file.json") as f:
        to_test = f.readlines()
    assert to_test == correct


def test_get_merge_commits_file():
    p = Parser(token="BxntftQ1zwq_28vtS2Qm")
    p.get_merge_commits_file(input_file="Data/get_merge_requests_file/output_get_merge_requests_file.json",
                             output_file="Data/get_merge_commits_file/test_output_get_merge_commits_file.csv")
    with open("Data/get_merge_commits_file/output_get_merge_commits_file.csv") as f:
        correct = f.readlines()
    with open("Data/get_merge_commits_file/test_output_get_merge_commits_file.csv") as f:
        to_test = f.readlines()
    assert to_test == correct


def test_get_all_features():
    p = Parser(token="BxntftQ1zwq_28vtS2Qm")
    path_input = Path("Data", "get_merge_commits_file", "output_get_merge_commits_file.csv")
    path_output = Path("Data", "get_all_features", "test_output_all_features.csv")
    path_test = Path("Data", "get_all_features", "output_all_features.csv")
    p.get_all_features_file(input_file=path_input,
                            output_file=path_output)
    with open(path_test) as f:
        correct = f.readlines()
    with open(path_output) as f:
        to_test = f.readlines()
    assert to_test == correct
    df = pd.read_csv(path_test, index_col=[0])
    assert list(df.columns) == ['project_id', 'commit_id', 'commit_message', 'files_changed',
                                'lines_inserted', 'lines_deleted', 'diff', 'target', 'changes', 'add',
                                'del', 'web_url', 'Imports added', 'Imports deleted', 'path',
                                'file_new', 'file_past', 'count_py_files', 'array_past', 'array_new',
                                'key_words_code_added', 'key_words_code_deleted', 'key_words'], "Не все колонки"
