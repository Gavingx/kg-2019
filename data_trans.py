# -*- coding:utf-8 -*-
# @Author: 苏神
# @File: data_trans.py
# @Time: 2019/9/24
# @Software: Pycharm
# @Desc: schema和训练验证数据转换
# @Modified_By：Gavin


import json
from tqdm import tqdm
import codecs
import traceback
import os, sys


project_path = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_path)


class DataTrans(object):
    """
    数据转换
    """
    def __init__(self):
        self.id2predicate = None
        self.predicate2id = None
        self.chars = {}
        self.chars_trans = None
        self.train_data = []
        self.dev_data = []
        self.min_count = 2
        self.id2char = None
        self.char2id = None

    def trans_train_data(self, in_file_path, out_file_path):
        """以特定格式处理数据

        :param in_file_path: 输入数据路径
            -type: 字符串

        :param out_file_path: 输出数据路径
            -type: 字符串

        :return: None

        """
        try:
            with codecs.open(in_file_path, 'r', encoding='utf-8') as f:
                for l in tqdm(f):
                    a = json.loads(l)
                    self.train_data.append(
                        {
                            'text': a['text'],
                            'spo_list': [(i['subject'], i['predicate'], i['object']) for i in a['spo_list']]
                        }
                    )
                    for c in a['text']:
                        self.chars[c] = self.chars.get(c, 0) + 1
            with open(out_file_path, 'w', encoding='utf-8') as file:
                json.dump(self.train_data, file, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f'加载数据出错：{traceback.format_exc()}!')

    def trans_dev_data(self, in_file_path, out_file_path):
        """以特定格式处理数据

        :param in_file_path: 输入数据路径
            -type: 字符串

        :param out_file_path: 输出数据路径
            -type: 字符串

        :return: None

        """
        try:
            with codecs.open(in_file_path, 'r', encoding='utf-8') as f:
                for l in tqdm(f):
                    a = json.loads(l)
                    self.dev_data.append(
                        {
                            'text': a['text'],
                            'spo_list': [(i['subject'], i['predicate'], i['object']) for i in a['spo_list']]
                        }
                    )
                    for c in a['text']:
                        self.chars[c] = self.chars.get(c, 0) + 1
            with open(out_file_path, 'w', encoding='utf-8') as file:
                json.dump(self.dev_data, file, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f'加载数据出错：{traceback.format_exc()}!')

    def trans_schema_data(self, in_file_path, out_file_path):
        """加载schema数据

        :param in_file_path: schema数据路径
            - type: string

        :param in_file_path: 转换后的schema数据路径
            - type: string

        :return:
            id2predicate：id转p
                -type：dict
            predicate2id：p转id
                -type：dict
        """
        try:
            all_50_schemas = set()
            with open(in_file_path, 'r', encoding='utf-8') as f:
                for l in tqdm(f):
                    a = json.loads(l)
                    all_50_schemas.add(a['predicate'])
            self.id2predicate = {i: j for i, j in enumerate(all_50_schemas)}
            self.predicate2id = {j: i for i, j in self.id2predicate.items()}
            with codecs.open(out_file_path, 'w', encoding='utf-8') as file:
                json.dump([self.id2predicate, self.predicate2id], file, indent=4, ensure_ascii=False)
            return self.id2predicate, self.predicate2id
        except Exception as e:
            print(f"schema数据转换错误: {traceback.format_exc()}")

    def trans_chars_data(self, out_file_path):
        """转换char数据

        :param out_file_path: 输出数据路径
            -type: string

        :return: None
        """
        try:
            with codecs.open(out_file_path, 'w', encoding='utf-8') as f:
                self.chars_trans = {i: j for i, j in self.chars.items() if j >= self.min_count}
                self.id2char = {i+2: j for i, j in enumerate(self.chars)} #padding:0, unk:1
                self.char2id = {j: i for i, j in self.id2char.items()}
                json.dump([self.id2char, self.char2id], f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"chars数据转换错误: {traceback.format_exc()}")

    def trans_data(self, kwargs):
        """转换所有数据

        :param kwargs: 训练数据输入输出路径，验证数据输入输出路径，schema输入输出路径等
            -type: dict
        :return:
        """
        self.trans_schema_data(kwargs['schema_in_path'], kwargs['schema_out_path'])
        self.trans_train_data(kwargs['train_in_path'], kwargs['train_out_path'])
        self.trans_dev_data(kwargs['dev_in_path'], kwargs['dev_out_path'])
        self.trans_chars_data(kwargs['chars_out_path'])

    @staticmethod
    def trans_total_data(out_path, *args):
        """将所有的数据合并在一起

        :param out_path: 合并后的路径
            -type: string

        :param args: 数据集路径
            -type: string

        :return:
        """
        try:
            data = []
            num = 0
            with open(out_path, 'a+', encoding='utf-8') as file:
                for path in args:
                    with open(path, 'r', encoding='utf-8') as f:
                        a = json.load(f)
                        num += len(a)
                        data += a
                json.dump(data, file, indent=4, ensure_ascii=False)
            print(len(data))
            if len(data) != 194747:
                print('长度不一致')
            else:
                print('长度一致')
        except Exception as e:
            print(f'数据转换错误：{traceback.format_exc()}')


if __name__ == '__main__':
    path_config = {
        'schema_in_path': os.path.join(project_path, 'datasets/all_50_schemas'),
        'schema_out_path': os.path.join(project_path, 'datasets/all_50_schemas_me.json'),
        'train_in_path': os.path.join(project_path, 'datasets/train_data.json'),
        'train_out_path': os.path.join(project_path, 'datasets/train_data_me.json'),
        'dev_in_path': os.path.join(project_path, 'datasets/dev_data.json'),
        'dev_out_path': os.path.join(project_path, 'datasets/dev_data_me.json'),
        'chars_out_path': os.path.join(project_path, 'datasets/all_chars_me.json')
    }
    # 转换所有数据
    data = DataTrans()
    data.trans_data(path_config)

    # 将训练集、验证集合并
    out_path = os.path.join(project_path, 'datasets/train_data_vote_me.json')
    DataTrans.trans_total_data(out_path, path_config['train_out_path'], path_config['dev_out_path'])
