import re
import json
import datetime
import numpy as np
import pandas as pd

import json
import requests

from ast import literal_eval

import io
import zipfile

import time
from tqdm import tqdm

import pytz
import datetime

import gitlab


class Parser:
    """
    Данный модуль представляет собой парсер коммитов гитлаба.
    Реализованы методы, которые взаимодейтвуют с файлами и с вебом.
    """
    def __init__(self, token=""):
        """
        Функция инициализации парсера, принимает токен для обращения к api гитлаба
        :param token: если токен не указывается, значение по умолчанию - пустая строка,
        что соответсвует доступу только к открытым проектам
        """
        self.TOKEN = token
        self.HEADERS = {'Authorization': 'Bearer ' + token}

        self.project_id_out_of_func = 0
        self.t = pd.DataFrame()

    def __handle_commits(self, project_id, commits, df: pd.DataFrame):
        """
        Функция обрабатывает коммиты и возвращает датафрейм с основными признаками
        :param project_id: id проекта в гитлабе
        :param commits: коммиты проекта
        :param df: датафрейм для изменения
        :return: датафрейм с основными признаками
        """
        if type(commits) is dict and commits.get("message", "fine") != "fine" or len(commits) == 0:
            return None
        for commit in commits:
            commit_id = commit["short_id"]  # берем id коммита
            message = commit["message"]  # берем message
            commit_stats = requests.get(
                f"https://git.miem.hse.ru/api/v4/projects/{project_id}/repository/commits/{commit_id}",
                headers=self.HEADERS).json().get("stats")
            additions = commit_stats["additions"]
            deletions = commit_stats["deletions"]
            # print(project_id, iid, commit_id)
            diffs = requests.get(
                f"https://git.miem.hse.ru/api/v4/projects/{project_id}/repository/commits/{commit_id}/diff",
                headers=self.HEADERS).json()  # здесь diffs во всех измененных файлах
            changes = []
            for item in diffs:  # берем имя файла и diff
                changes.append({
                    "new_path": item["new_path"],
                    "diff": item["diff"]
                })
            files_changed = len(changes)
            new_row = {
                "project_id": project_id,
                "commit_id": commit_id,
                "commit_message": message,
                "files_changed": files_changed,
                "lines_inserted": additions,
                "lines_deleted": deletions,
                "diff": changes
            }  # создаем новую строку
            df.loc[len(df)] = new_row

        return df

    def __get_advanced_data(self, df):
        """
        Функция достает дополнительные признаки для предоставленных данных
        :param df: датафрейм с основными признаками
        :return: датафрейм с дополнительными признаками
        """
        if df is None:
            return None

        def count_lines(string):
            count = 0
            for el in string.split(';'):
                if not re.fullmatch('[ ]*', el):
                    count += 1
            return count

        df['changes'] = ''
        df['add'] = ''
        df['del'] = ''
        df['web_url'] = ''
        df[['web_url', 'changes']] = df.apply(lambda row: self.get_data(row['project_id'], row['commit_id']), axis=1)
        df[['add', 'del']] = df.apply(lambda row: self.get_difference(row['changes']), axis=1)
        df['files changed'] = 0
        df['files changed'] = df['changes'].apply(lambda x: self.count_files(x))
        df['Imports added'] = 0
        df['Imports deleted'] = 0
        df['add'].fillna('', inplace=True)
        df['del'].fillna('', inplace=True)

        df['Imports added'] = df['add'].apply(lambda x: self.find_import(x))
        df['Imports deleted'] = df['del'].apply(lambda x: self.find_import(x))

        # df['diff'] = df['diff'].apply(lambda x: literal_eval(x))
        df['path'] = df['diff'].apply(lambda x: self.get_path(x))

        df['file_new'] = ''
        df['file_past'] = ''

        df = df[df['files changed'] > 0]

        df.reset_index(drop=True, inplace=True)

        df['count_py_files'] = 0
        df['count_py_files'] = df['path'].apply(lambda x: self.count_py_file(x))

        df['flag'] = 0
        df['array_past'] = 0
        df['array_new'] = 0
        df['key_words_code_added'] = df['add'].apply(lambda x: self.find_key_word_code(x))
        df['key_words_code_deleted'] = df['add'].apply(lambda x: self.find_key_word_code(x))
        df["key_words"] = df["commit_message"].apply(lambda x: self.find_key_words_message(x))
        df = df[df.count_py_files > 0]
        df.reset_index(drop=True, inplace=True)
        if len(df) == 0:
            return df
        df[['file_past', 'file_new', 'array_past', 'array_new', 'flag']] = df.apply(
            lambda row: self.get_whole_file(row['project_id'],
                                            row['commit_id'],
                                            row['path']), axis=1)
        df = df.fillna(' ;')

        df['lines_inserted'] = df['add'].apply(func=count_lines)
        df['lines_deleted'] = df['del'].apply(func=count_lines)
        df = df[df["file_past"].apply(str).apply(len) > 20]
        df = df[df["file_new"].apply(str).apply(len) > 20]
        df.drop(["flag", "files changed"], axis=1, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def get_merge_requests_file(self, input_file: str, output_file: str) -> None:
        """
        Функция достает мердж реквесты из файла
        :param input_file: путь до входного файла
        :param output_file: путь до выходного файла
        :return:
        """
        __projects = set()

        def search_for_merges(data):
            x = lambda a: a.lower()
            projects = data["projects"]
            for project in projects:
                if "python" in list(map(x, project["languages"].keys())):
                    __projects.add(project["id"])
            for project_id in __projects:
                try:
                    url = f"https://git.miem.hse.ru/api/v4/projects/{project_id}/merge_requests"
                    response = requests.get(url, headers=self.HEADERS)
                    merge = response.json()
                    # if type(merge) != type([]) and "404" in merge["message"]:
                    #    output.write(url)
                    # else:
                    json.dump(merge, output)
                    output.write("\n")
                except:
                    continue

        output = open(output_file, "w")

        with open(input_file, "r", encoding="UTF-8") as f:
            line = f.readline()
            while line:
                data = json.loads(line)
                search_for_merges(data)
                line = f.readline()

        output.close()

    def get_merge_commits_file(self, input_file: str, output_file: str, limit=0) -> None:
        """
        Функция обрабатывает мердж коммиты и записывает их в файл
        :param input_file: путь до входного файла
        :param output_file: путь до выходного файла
        :param limit: ограничение по коммитам
        :return:
        """

        df = pd.DataFrame(data=None, columns=[
            "project_id",
            "commit_id",
            "commit_message",
            "files_changed",
            "lines_inserted",
            "lines_deleted",
            "diff"
        ])

        def add_data(data):  # data - это список merge requests, у каждого такого request есть много коммитов
            for merge_request in data:
                project_id = merge_request["project_id"]  # берем id проекта
                iid = merge_request["iid"]  # берем iid мерджа
                commits = requests.get(
                    f"https://git.miem.hse.ru/api/v4/projects/{project_id}/merge_requests/{iid}/commits",
                    headers=self.HEADERS).json()  # Получаем коммиты
                for commit in commits:
                    commit_id = commit["short_id"]  # берем id коммита
                    message = commit["message"]  # берем message
                    commit_stats = requests.get(
                        f"https://git.miem.hse.ru/api/v4/projects/{project_id}/repository/commits/{commit_id}",
                        headers=self.HEADERS).json().get("stats")
                    additions = commit_stats["additions"]
                    deletions = commit_stats["deletions"]
                    # print(project_id, iid, commit_id)
                    diffs = requests.get(
                        f"https://git.miem.hse.ru/api/v4/projects/{project_id}/repository/commits/{commit_id}/diff",
                        headers=self.HEADERS).json()  # здесь diffs во всех измененных файлах
                    changes = []
                    for item in diffs:  # берем имя файла и diff
                        changes.append({
                            "new_path": item["new_path"],
                            "diff": item["diff"]
                        })
                    files_changed = len(changes)
                    new_row = {
                        "project_id": project_id,
                        "commit_id": commit_id,
                        "commit_message": message,
                        "files_changed": files_changed,
                        "lines_inserted": additions,
                        "lines_deleted": deletions,
                        "diff": changes
                    }  # создаем новую строку
                    df.loc[len(df)] = new_row  # добавляем новую строку в df

        with open(input_file, "r", encoding="UTF-8") as f:
            line = f.readline()
            while (line and limit == 0) or (line and len(df) < limit):
                data = json.loads(line)
                add_data(data)
                line = f.readline()

        df.to_csv(output_file)

    def get_all_features_file(self, input_file: str, output_file=None) -> None:
        """
        Добавляет дополнительные признаки к сущесвтвющим коммитам
        :param input_file: путь до входного файла
        :param output_file: путь до выходного файла
        :return:
        """
        if output_file is None:
            output_file = input_file
        df = pd.read_csv(input_file, index_col=[0])
        df["target"] = np.nan
        self.__get_advanced_data(df).to_csv(output_file)

    @staticmethod
    def get_difference(changes):
        """
        Возвращает добавленный/удаленный код в коммите на основе diff
        :param changes: колонка changes в коммите
        :return:
        """
        deleting = ''
        adding = ''
        for i in range(len(changes)):
            diff = changes[i]['diff']
            coor = [m.start() for m in re.finditer('@@', diff)]
            if len(coor) == 0:
                continue
            else:
                n = int((len(coor[1:-1])) / 2)
                for i in range(n):
                    i *= 2
                    diff_i = diff[coor[i + 1]:coor[i + 2]]
                    plus = [m.start() for m in re.finditer('\n\+', diff_i)]
                    minus = [m.start() for m in re.finditer('\n\-', diff_i)]
                    if len(plus) == 0:
                        continue
                    else:
                        for i in range(len(plus)):
                            to_add = diff_i[plus[i] + 2: plus[i] + 2 + diff_i[plus[i] + 2:].find('\n')]
                            if len(to_add):
                                adding += to_add
                                adding += ';'
                    if len(minus) == 0:
                        continue
                    else:
                        for i in range(len(minus)):
                            to_del = diff_i[minus[i] + 2: minus[i] + 2 + diff_i[minus[i] + 2:].find('\n')]
                            if len(to_del) > 0:
                                deleting += to_del
                                deleting += ';'
                diff_end = diff[coor[-1]:]
                plus = [m.start() for m in re.finditer('\n\+', diff_end)]
                minus = [m.start() for m in re.finditer('\n\-', diff_end)]
                if len(plus) == 0:
                    continue
                else:
                    for i in range(len(plus)):
                        to_add = diff_end[plus[i] + 2: plus[i] + 2 + diff_end[plus[i] + 2:].find('\n')]
                        if len(to_add) > 0:
                            adding += to_add
                            adding += ';'
                if len(minus) == 0:
                    continue
                else:
                    for i in range(len(minus)):
                        to_del = diff_end[minus[i] + 2: minus[i] + 2 + diff_end[minus[i] + 2:].find('\n')]
                        if len(to_del) > 0:
                            deleting += to_del
                            deleting += ';'

        return pd.Series([adding, deleting])

    def get_changes(self, project_id_, commit_id_):
        """
        Возвращает необходимую информацию (структура словаря: diff, commit_message etc.) полученную по конкретному коммиту
        :param project_id_: id проекта
        :param commit_id_: id коммита
        :return:
        """
        response = requests.get(
            f"https://git.miem.hse.ru/api/v4/projects/{project_id_}/repository/commits/{'commit_id_'}/diff",
            headers=self.HEADERS).json()
        return response['web_url']

    def get_data(self, project_id_, commit_id_):
        """
        Возвращает ссылку на коммит на гитлабе, diff коммита
        :param project_id_: id проекта
        :param commit_id_: id коммита
        :return:
        """
        response_1 = requests.get(
            f"https://git.miem.hse.ru/api/v4/projects/{project_id_}/repository/commits/{commit_id_}",
            headers=self.HEADERS).json()
        response_2 = requests.get(
            f"https://git.miem.hse.ru/api/v4/projects/{project_id_}/repository/commits/{commit_id_}/diff",
            headers=self.HEADERS).json()
        commit = {}
        commit["changes"] = []
        for item in response_2:
            commit["changes"].append({
                "diff": item["diff"]
            })

        return pd.Series([response_1['web_url'], commit["changes"]])

    @staticmethod
    def count_files(changes):
        """
        Возвращает кол-во файлов, измененных в коммите
        :param changes: diff коммита
        :return:
        """
        return len(changes)

    @staticmethod
    def find_import(add):
        """
        Возвращает кол-во добавленных импортов в коммите
        :param add: добавленные строки в коммите
        :return:
        """
        return len([m.start() for m in re.finditer('import', add)])

    @staticmethod
    def find_key_word_code(code):
        """
        Возвращает кол-во ключевых слов (def, class)
        :param code: добавленные или удаленные строки в коммите
        :return:
        """
        d = {}
        d["def"] = len([m.start() for m in re.finditer('def', code)])
        d["class"] = len([m.start() for m in re.finditer('class', code)])
        return d

    @staticmethod
    def find_key_words_message(message):
        """
        Возвращает ключевые слов
        :param message: commit message
        :return:
        """
        key_words = ["remove", "merge branch", "fix", "add",
                         "update", "change", "release", "correct", "replace",
                         "deleted", "refactor", "clean", "test", "minor", "prepar",
                         "move", "feature", "optimization", "resolv", "improve",
                         "feat", "rename", "debug"]
        res = []
        temp = message.split()
        for key_word in key_words:
            for word in temp:
                if word.lower().startswith(key_word):
                    if key_word == "prepar":
                        res.append("prepare")
                    if key_word == "resolv":
                        res.append("resolve")
                    res.append(key_word)
        return res

    @staticmethod
    def get_path(diff):
        """
        Возвращает массив путей до файлов, которые были изменены в коммите
        :param diff: diff коммита
        :return:
        """
        false_commit = ''
        paths = []
        if isinstance(diff, str):
            diff = eval(diff)
        for commit in diff:
            try:
                # paths += commit['new_path'] + ';;'
                paths.append(commit['new_path'])
            except:
                false_commit += str(diff)
                # paths += ''
                # print(commit)
        return paths

    @staticmethod
    def convert_time(date_str):
        """
        Конвертирует даты к одному формату (гитлаб выдает даты в разном формате).
        :param date_str: даты в коммите
        :return:
        """
        date = datetime.datetime.fromisoformat(date_str)
        utc = pytz.UTC
        date = date.astimezone(utc)
        return date

    @staticmethod
    def count_py_file(path_files):
        """
        Возвращает кол-во питоновских файлов, измененных в коммите
        :param path_files: пути до файлов, измененных в коммите
        :return:
        """
        subs = '.py'
        subs_1 = '.pyc'
        return len(list(filter(lambda x: (subs in x) and (subs_1 not in x), path_files)))

    # эти две global переменные важны, они используются в функции ниже
    project_id_out_of_func = 0
    t = pd.DataFrame()

    def get_whole_file(self, project_id, commit_sha, file_names):
        """
        Возварщает изначальный код файлов в виде одной строки, новый код файлов в виде одной строки
                массив: путь до файла - его код (до и после коммита), проверка на совпадение кода "до" и "после" коммита
        :param project_id: id проекта
        :param commit_sha: sha коммита
        :param file_names: пути до файлов измененных в этом коммите
        :return:
        """

        stop = ['gitignore',
                'flake8',
                'pyc',
                'ipynb',
                'vue']

        GITLAB_URL = 'https://git.miem.hse.ru/'
        ACCESS_TOKEN = self.TOKEN
        gl = gitlab.Gitlab(GITLAB_URL, ACCESS_TOKEN)
        gl.auth()
        project = gl.projects.get(project_id)
        commits = project.commits.list(get_all=True, all=True)

        array_new = []
        array_past = []

        file_new = ''
        file_past = ''
        flag = 0

        # global project_id_out_of_func
        # global t

        if project_id != self.project_id_out_of_func:
            self.t = pd.DataFrame(commits)
            self.t['time'] = self.t[0].apply(lambda x: x.created_at)
            self.t.sort_values(by='time', ascending=False, inplace=True)
            self.t.reset_index(drop=True, inplace=True)
            self.t['sha'] = self.t[0].apply(lambda x: x.id[:8])
            self.t['title'] = self.t[0].apply(lambda x: x.title)

        for file_name in tqdm(file_names):

            file_new_with_name = ''
            file_past_with_name = ''
            flag = 0
            d_new = {}
            d_past = {}

            df = self.t.copy(deep=True)

            if ('.py' in file_name) and ('.pyc' not in file_name):
                arc = project.repository_archive(sha=commit_sha, format='zip')
                # print(commit_sha)
                zf = zipfile.ZipFile(io.BytesIO(arc), "r")
                for i in range(len(zf.infolist())):
                    if file_name in zf.infolist()[i].filename:
                        try:
                            file_new += zf.read(zf.infolist()[i]).decode('utf-8') + '\n;\n'
                            file_new_with_name += zf.read(zf.infolist()[i]).decode('utf-8') + '\n;\n'
                        except:
                            file_new += ''
                            file_new_with_name += ''

                d_new[file_name] = file_new_with_name

                past_sha = ''
                try:
                    parent_id = df[df.sha == commit_sha][0].values[0].parent_ids[0]
                    past_sha = parent_id[:8]
                except IndexError:
                    continue

                if past_sha != '':
                    arc = project.repository_archive(sha=past_sha, format='zip')
                    zf = zipfile.ZipFile(io.BytesIO(arc), "r")
                    for i in range(len(zf.infolist())):
                        if file_name in zf.infolist()[i].filename:
                            try:
                                file_past += zf.read(zf.infolist()[i]).decode('utf-8') + '\n;\n'
                                file_past_with_name += zf.read(zf.infolist()[i]).decode('utf-8') + '\n;\n'
                            except:
                                file_past += ''
                                file_past_with_name += ''

                d_past[file_name] = file_past_with_name
                array_new.append(d_new)
                array_past.append(d_past)

                if file_new == file_past or file_new == "" or file_past == "":
                    flag = 1
                else:
                    flag = 0
        return pd.Series([file_past, file_new, array_past, array_new, flag])

    def get_project_commits(self, project_id: str):
        """
        Возвращает коммиты проекта
        :param project_id: id проекта
        :return: список коммитов проекта
        """
        url = f"https://git.miem.hse.ru/api/v4/projects/{project_id}/repository/commits"
        response = requests.get(url=url, headers=self.HEADERS)
        commits = response.json()
        return commits

    def get_project_commits_with_basic_features_web(self, project_id: str):
        """
        Возвращает коммиты проекта с базовыми признаками в виде pandas DataFrame.
        :param project_id: id проекта
        :return: датафрейм с базовыми признаками
        """
        commits = self.get_project_commits(project_id)
        df = pd.DataFrame(data=None, columns=[
            "project_id",
            "commit_id",
            "commit_message",
            "files_changed",
            "lines_inserted",
            "lines_deleted",
            "diff",
            "target"
        ])
        return self.__handle_commits(project_id, commits, df)

    def get_project_commits_with_all_features_web(self, project_id: str):
        """
        Возвращает коммиты проекта со всеми признаками
        :param project_id: id проекта
        :return: датафрейм со всеми признаками
        """
        df = self.get_project_commits_with_basic_features_web(project_id)
        return self.__get_advanced_data(df)

    def get_project_merge_requests_web(self, project_id: str):
        """
        Вернуть мердж реквесты проекта
        :param project_id: id проекта
        :return: список мердж реквестов
        """
        url = f"https://git.miem.hse.ru/api/v4/projects/{project_id}/merge_requests"
        response = requests.get(url=url, headers=self.HEADERS)
        merges = response.json()
        return merges

    def get_merge_commits_web(self, project_id, iid=-1):
        """
        Возвращает коммиты мердж реквеста с базовыми функциями
        если iid равен -1, то возвращает все коммиты мердж реквеста
        иначе возвращает коммиты определенного мердж реквеста
        :param project_id: id проекта
        :param iid: id мердж реквеста
        :return:
        """
        df = pd.DataFrame(data=None, columns=[
            "project_id",
            "commit_id",
            "commit_message",
            "files_changed",
            "lines_inserted",
            "lines_deleted",
            "diff",
            "target"
        ])
        if iid == -1:
            merges = self.get_project_merge_requests_web(project_id)
            for merge in merges:
                commits = requests.get(
                    f"https://git.miem.hse.ru/api/v4/projects/{project_id}/merge_requests/{merge['iid']}/commits",
                    headers=self.HEADERS).json()
                df = self.__handle_commits(project_id, commits, df)
                return self.__get_advanced_data(df)

        else:
            commits = requests.get(f"https://git.miem.hse.ru/api/v4/projects/{project_id}/merge_requests/{iid}/commits",
                                   headers=self.HEADERS).json()  # Получаем коммиты
            df = self.__handle_commits(project_id, commits, df)
            return self.__get_advanced_data(df)
