import json
import sqlite3
from datetime import datetime

import pandas as pd
from openai import OpenAI

from dotenv import load_dotenv
import os

# import viz
from utils.dates import mtds_generate
from utils.parser import parse_code_blocks, parse_sql, parse_sql_intro

db_dir='data'
db='pms.db'
engine='sqlite'
conn = sqlite3.connect(f'{db_dir}/{db}', check_same_thread=False) #harusnay uda on pas initiate atau async pas hit query
list_tables=['pms_daily', 'pms_detailed_monthly'] #daily only dpk and loan, monthly all but dpk and loan (covered on daily)

# table = 'pms_daily'
table = 'pms_daily'

date_col = 'as_of_date'

q_min_max_date = f'SELECT min({date_col}) as min_date, max({date_col}) as max_date FROM {table} order by {date_col}'
min_max_date = pd.read_sql(q_min_max_date,conn).iloc[0]
first_avail_date, last_avail_date= min_max_date['min_date'], min_max_date['max_date']

# min_max_date_opex = pd.read_sql(f'SELECT min({date_col}) as min_date, max({date_col}) as max_date FROM {table} WHERE OPEX is not NULL order by {date_col}',conn).iloc[0]
# first_avail_date_opex, last_avail_date_opex= min_max_date_opex['min_date'], min_max_date_opex['max_date']
# print('\n\n\nfirst last date in data:\n',first_avail_date_opex, last_avail_date_opex)
#no column mapping correspondence needed no more as the column names are now carefully polished to represent the col values
additional_info=f"""Hari ini: {str(datetime.now())[:10]}.
Data daily tersedia dari {first_avail_date} sampai {last_avail_date}.

Untuk mencari nama cabang atau segmen, selalu gunakan LIKE '%xxx$', misal dicari BOGOR maka "WHERE branch_name LIKE '%BOGOR%'"

Nominal dalam Rupiah.
"""
# table={}
few_shots={}
system_message={}
few_shots['table_picker']=f'''
    Q: ada berapa saldo rekening dari cabang palembang?
    A: dpk
    Q: totalkan jumlah pengguna mbank yang ebrtempat di wialayah bsd
    A: mbank_uppermass
    Q: berpaa total loan dari korporasi?
    A: wholesale_loan_menengah
    '''

list_of_mtds = mtds_generate(last_avail_date)

print('list of datesss',str(list_of_mtds)[1:-1],'\n\n\n\n\n')
few_shots['query_'+table]=f'''
    Q: Wilayah mana yang memiliki loan indopex paling tinggi
    A: Let's think step by step. Assume the info to be used is last date available and because no specific period is given, values to be considered are balance / YtD values.

    Understanding the Data:

    Table PMS has daily branch performance data
    We have x dates
    Multiple rows may exist per branch per day (segments, etc)
    INDOPEX column is YtD columns, while LOAN is positional balance. We can just query the latest date to get YtD for INDOPEX and LOAN position.

    What we need to find:

    Highest INDOPEX branch
    Highest LOAN branch
    Need to sum these because one branch has multiple entries

    Logical steps:

    First sum up the values per branch at the latest date available
    Then rank branches based on these sums
    Pick the highest ranked one

    assumptions: No exact numbers given, assume top 3 (for each criterion).
    notes: exclude NULL in grouped columns.

    ```sql
    WITH LoanRank AS (
        SELECT
            REGION_NAME,
            BRANCH_NAME,
            SUM(loan) AS loan,
            ROW_NUMBER() OVER (ORDER BY opex DESC) as loan_rank
        FROM {table}
        WHERE TANGGAL = '{last_avail_date}'
            AND REGION_NAME IS NOT NULL
            AND BRANCH_NAME IS NOT NULL
        GROUP BY 1, 2
    ),
    IndopexRank AS (
        SELECT
            REGION_NAME,
            BRANCH_NAME,
            SUM(indopex) AS REAL_INDOPEX_YTD,
            ROW_NUMBER() OVER (ORDER BY indopex DESC) as indopex_rank
        FROM {table}
        WHERE TANGGAL= '{last_avail_date}'
            AND REGION_NAME IS NOT NULL
            AND BRANCH_NAME IS NOT NULL
        GROUP BY 1, 2
    )
    SELECT
        'Highest LOAN' as Category,
        CONCAT(BRANCH_NAME, ' - ', REGION_NAME) as Branch_Region,
        loan as Value
    FROM    LoanRank
    WHERE loan_rank <= 3
    UNION ALL
    SELECT
        'Highest INDOPEX' as Category,
        CONCAT(BRANCH_NAME, ' - ', REGION_NAME) as Branch_Region,
        indopex as Value
    FROM IndopexRank
    WHERE indopex_rank <= 3
    ORDER BY Category;```
'''
cols_info = '''
Columns information:
1. Balance/Position Metrics (End of Period):
   - dpk
   - loan
   Sample balance col: 
   as_of_date | value
   _______________________
    2024-01-31 | 450000
    2024-02-29 | 465000
    2024-03-31 | 475000

2. Year to Date metrics:
   - fee_based_income (or FBI)
   - interest_expense
   - interest_income
   - indopex
   - opex
   - provision
      Sample YtD col: 
   as_of_date | fee_based_income
   _______________________
    2024-01-31 | 1000
    2024-02-29 | 2100
    2024-03-31 | 3000

   As all columns represent the position for the metric of the day, only query the date of interest, 
   unless specific monthly or daily change is needed, then do subtraction as needed. 
   For example: when asked every month's DPK, then pick filter of as_of_date for each end of month. However, when the change / improvement is asked, then do substraction between end date and start date values.
   Granular hierarchy (each row corresponds to the lowest level of granularity):
    a. daerah (wilayah di atas cabang)
    - REGION_NAME
    - branch_name
    b. segment
    - segment_div_owner
    antara daerah dan segmen tidak ada hubungan hirarki, sehingga tiap cabang punya banyak segmen dan kebalikannya.
Note: Balance metrics represent point-in-time values, while Monthly Flow metrics are cumulative within each month.
'''


system_message['query_'+table]=f'''

    {cols_info}

    {additional_info}
    '''
system_message['modify_df']= '''
       ### Role
        You are a Pandas expert.
        ### Task
        Your task is to generate Pandas code to modify a Pandas dataframe table according to user's request.
        ### Instructions
        You break down the steps needed in order to fulfill the requirement,
        then you generate Pandas code accordingly.
        The Pandas dataframe table is readily available in variable df, therefore no need to rewrite sample data.

        ```python
        def modify_df(df):
            # You complete the code to modify df inplace
            return df
        ```
        '''

system_message['table_picker']= f'''
        ### Role
        You pick the most suitable table for a given query. You pick one table from {str(list_tables)},

        ### Table description
        wholesale_loan_menengah: Loan dari perusahaan yang meminjam di BNI
        mbank_uppermass: User mobile banking BNI (meliputi mobile banking lama dan Wondr (baru))
        dpk: Dana pihak ketiga (tabungan, deposito, giro) dari nasabah BNI
        '''


# load api key
load_dotenv()
openai_api=os.environ.get('OPENAI_API_KEY')
ope_client = OpenAI(api_key=openai_api)

class step1: #jadinya semua digabung disinmi aja, jadi ga cmn step 1
    def __init__(self, few_shot=few_shots, system_message=system_message, model="gpt-4o", verbose=1):
        # self.table_of_interest = table_of_interest
        self.system_message = system_message
        self.model = model
        self.model_table_picker= 'gpt-4o-mini'
        self.verbose = verbose
        self.query = ""
        self.query_intro = ""
        self.few_shot=few_shot
        self.modify_pandas_code = ''
        self.df = None
        self.modified_df = None
        self.engine = 'sqlite'
        # self.what_table='wholesale_loan_menengah'

    def modify_table_generator(self, modify_prompt):
        completion = ope_client.chat.completions.create(
            model=self.model,
            temperature=0,
            messages=[
                {"role": "system", "content": self.system_message['modify_df']},
                {"role": "user", "content": f'''
                tabel: {self.df}
                modify request: {modify_prompt}
                '''}
            ]
        )
        self.modify_pandas_code = parse_code_blocks(completion.choices[0].message.content)[0]
        print('generated modify df code:',self.modify_pandas_code)
        namespace={'pd':pd}
        exec(self.modify_pandas_code, namespace)
        print('namespace modify df:',namespace['modify_df'])
        modify_df = namespace['modify_df']
        self.modified_df = modify_df(self.df)
        self.df = self.modified_df.copy()
        return self.modified_df

    def table_picker(self):
        pick_table_completion = ope_client.chat.completions.create(
                model=self.model_table_picker,
                temperature=0,
                messages=[
                    {"role": "system", "content": self.system_message['table_picker']},
                    {"role": "user", "content": self.few_shot['table_picker']+r'\nQ: {}\nA:'.format(self.question)}
                ]
        ).choices[0].message.content

        # print('picked table:',pick_table_completion)
        # self.what_table= pick_table_completion
        self.what_table='pms_daily' #ini ga pake pick2an ae lah lgsg
        # self.what_table='pms_daily' #ini ga pake pick2an ae lah lgsg

        if 'unavailable' in pick_table_completion:
            return 'unavailable'
        else:
            return pd.read_sql_query(f'select * from {self.what_table} limit 1000', conn).sample(5)


    def generate_prefix_query_generator(self,sample_table_masked):
        string_prefix=f'''
            You are an executable {self.engine} query generator.
            Think step-by-step, then write a {self.engine} query that answers (and optionally) elaborates on a user's question; only after all your thoughts and planning for the query have been written. No need for further explanation after a query is written.
            When doing ranking/comparison/top, exclude NULL, empty string, and 'NULL' in the group by columns.
            Always include period (date) column whenever relevant, when talking about periodic (month/year), always use the last date of that month / year as reference.
            Always SUM() columns that contain numerical value because there's no smallest granularity in the data.
            table name: {self.what_table}

            write your query according to the data sample below:
            sample:
            {sample_table_masked}

            ONLY write query using columns on the sample table.
        '''

        self.prefix_query_generator=string_prefix

    def stream_query_gen(self, question):
        self.question=question
        sample_table_masked=self.table_picker()
        print('a')
        if type(sample_table_masked)==str:
            yield 'Table unavailable'
            # pass
        else:
            self.generate_prefix_query_generator(sample_table_masked)
            print('prefix query terpilih:',self.prefix_query_generator)
            if self.model[:3] == 'gpt':
                completion = ope_client.chat.completions.create(
                    model=self.model,
                    temperature=0,
                    stream=True,
                    messages=[
                        {"role": "system", "content": "You are an executable {self.engine} query generator."},
                        {"role": "user", "content": self.prefix_query_generator+self.system_message['query_'+self.what_table]+self.few_shot['query_'+self.what_table]+r'\nQ: {}\nA:'.format(question)}
                    ]
                )

                full_response = ""
                # mulai_kirim=0
                for chunk in completion:
                    if chunk.choices[0].delta.content is not None:
                        content = json.dumps(chunk.choices[0].delta.content)[1:-1]
                        full_response += content
                        yield content
                        # print('content:',content)
                if self.verbose:
                    print(full_response)

                parsed_query = parse_sql(full_response)

                self.query_intro = parse_sql_intro(full_response)
                if parsed_query == 'no queries':
                    self.query = None
                else:
                    self.query = parsed_query.replace('\\n', ' ')

                    
                
    def exec_q(self):
        if not self.query:
            print('no queries, self.query = ', self.query)
            return None

        else:
            self.query=self.query.replace('\"', '')
            self.df = pd.read_sql(self.query, conn)

            if self.verbose:
                print(self.df)

            return self.df

#     def generate_and_save_viz(self):
#         system_prompt_chart_picker='''You will be given a sample of a table and a user question, and
#         you decide whether it is best visualized with:
#         if asked about trends or top values: bar chart ('bar'),
#         if distribution/composition or comparing values: pie chart ('pie'), or
#         if columns are less than two: cannot be visualized ('unavailable')

#         and

#         you decide what columns should be used for each X (category) and Y (value) axes.
#         '''
#         completion = ope_client.chat.completions.create(
#             model='gpt-4o',
#             temperature=0,
#             messages=[
#                 {"role": "system", "content": system_prompt_chart_picker},
#                 {"role": "user", "content": f'''
#                 q: ada berapa banyak debitur selama 4 bulan terakhir
#                 table:
#                 PERIOD  JMLDEB
#                 2023-12-31    2539
#                 2024-01-31    2488
#                 2024-02-29    2428
#                 2024-03-31    2424
#                 chart: bar
#                 x: PERIOD
#                 y: JMLDEB
#                 q: berapa total interest income dari cabang2 di medan?
#                 table:
#                 periode  branch  pendapatan_bunga
#                 2024-01-31  Mekarsa 3213000
#                 2024-01-31  Baduy   2000000
#                 2024-01-31  Ahoy    2300000
#                 2024-01-31  Bogor   1232222

#                 chart: pie
#                 x: branch
#                 y: pendapatan_bunga

#                 q: {self.query}
#                 table: {self.df}
#                 '''}
#             ]
#         )

#         viz_reasoned = completion.choices[0].message.content
#         print('viz decision:',viz_reasoned)
#         if viz_reasoned!='unavailable':
#             viz_reasoned = {
#                 ['chart','x','y'][i]: el.split(':')[1].strip() for i,el in enumerate(viz_reasoned.split('\n'))
#             }
#             filepath='static/charts/chart_temp.png'
#             if viz_reasoned['chart']=='bar':
#                 # viz.bar(['a','b'],[1,0.5],filepath)

#                 viz.bar(self.df,viz_reasoned['x'],viz_reasoned['y'],filepath)
#             elif viz_reasoned['chart']=='pie':
#                 # viz.pie(['a','b'],[1,0.5],filepath)

#                 viz.pie(self.df,viz_reasoned['x'],viz_reasoned['y'],filepath)
#         else:
#             viz_reasoned={
#                 'chart': 'unavailable'
#             }
#         return viz_reasoned['chart']
    
# def stt(path):
#     audio_file= open(path, "rb")
#     transcription = ope_client.audio.transcriptions.create(
#         model="whisper-1",
#         file=audio_file,
#         prompt="opex, indopex",
#         language='id'
#     )
#     return transcription.text
