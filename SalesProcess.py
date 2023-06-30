import pandas as pd
import re
import time
import threading
import queue as q
from openpyxl import Workbook
import random


def format_date(data):
    regex = r"(\d{4})(\d{2})(\d{2})"
    subst = r"\3-\2-\1"

    nova_data = re.sub(regex, subst, data)
    return nova_data


class ConvertSales:
    def __init__(self, file_path):
        self.file_path = file_path
        self.sales_data_frame = pd.DataFrame()
        self.path_destination = '../reports/Relatorio-Vendas-{}.xlsx'
        self.path_destination_json = '../reports/Relatorio-Vendas-Parcial-{}.json'

    def process(self):
        print("Começou em: %s" % time.ctime())
        # for sales_df in pd.read_csv('sale-file.csv', sep=';', chunksize=2000):
        for sales_df in pd.read_csv(self.file_path, sep=';', chunksize=25):
            sales_df['modalidade'].replace('C', 'Crédito', inplace=True, )
            sales_df['modalidade'].replace('D', 'Débito', inplace=True)
            sales_df['modalidade'].replace('V', 'Voucher', inplace=True)

            data_venda = sales_df['data_venda'].apply(lambda x: format_date(str(x)))
            data_recebimento = sales_df['data_recebimento'].apply(lambda x: format_date(str(x)))
            sales_df['data_venda'] = data_venda
            sales_df['data_recebimento'] = data_recebimento

            self.sales_data_frame = pd.concat([self.sales_data_frame, sales_df])
            self.send_json_to_sqs(sales_df)

        print("Terminou em: %s" % time.ctime())

    def execute(self):
        # queue = q.Queue()
        # thread = threading.Thread(target=self.process, args=(queue,))
        # thread.start()

        # Espera a thread terminar de executar
        # thread.join()

        # Acessa o objeto retornado pela função
        # sales_df = queue.get()

        # if sales_df is None:
        #     raise Exception('A operação não foi concluída com sucesso')
        self.process()
        self.salve_excel()

    def salve_excel(self):
        print("Começou em: %s" % time.ctime())
        hs = random.randrange(1000, 1000000)
        file_name = self.path_destination.format(hs)
        self.sales_data_frame.to_excel(file_name)
        print("Terminou em: %s" % time.ctime())

    def send_json_to_sqs(self, sales_df):
        hs = random.randrange(1000, 1000000)
        file_name = self.path_destination_json.format(hs)
        sales_df.to_json(file_name)
        # json = sales_df.to_json(orient='records')
        # sqs.send_to_sqs(json)


cvs = ConvertSales('../temp/sale-file.csv')
cvs.execute()
