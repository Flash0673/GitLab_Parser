from Parser import Parser


def test_get_merge_requests_file():
    p = Parser(token="YOUR_TOKEN")
    p.get_merge_requests_file(input_file="Data/test.json",
                              output_file="Data/get_merge_requests_file/test_output_get_merge_requests_file.json")
    with open("Data/get_merge_requests_file/output_get_merge_requests_file.json") as f:
        correct = f.readlines()
    with open("Data/get_merge_requests_file/test_output_get_merge_requests_file.json") as f:
        to_test = f.readlines()
    assert to_test == correct


def test_get_merge_commits_file():
    p = Parser(token="YOUR_TOKEN")
    p.get_merge_commits_file(input_file="Data/get_merge_requests_file/output_get_merge_requests_file.json",
                             output_file="Data/get_merge_commits_file/test_output_get_merge_commits_file.csv")
    with open("Data/get_merge_commits_file/output_get_merge_commits_file.csv") as f:
        correct = f.readlines()
    with open("Data/get_merge_commits_file/test_output_get_merge_commits_file.csv") as f:
        to_test = f.readlines()
    assert to_test == correct
