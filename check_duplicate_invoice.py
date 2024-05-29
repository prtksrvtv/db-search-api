from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
import pandas as pd

load_dotenv()
engine = create_engine(os.environ['DATABASE_URL'])

def check_duplicate_invoice(bill_no, date_of_purchase, school_id):
    query = text(""" select bill_no from sales 
                 where bill_no=:bill_no 
                 and date_of_purchase=:date_of_purchase
                 and school_id=:school_id""")
    df=pd.read_sql_query(query, con=engine, params={'bill_no':bill_no, 'date_of_purchase':date_of_purchase, 'school_id':school_id})
    if len(df) == 0:
        return 200
    else:
        return 500