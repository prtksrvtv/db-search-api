from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import json
from babel.numbers import format_currency
from numtoword import *

load_dotenv()
engine = create_engine(os.environ['DATABASE_URL'])
app = Flask(__name__)

@app.route('/db_product_search', methods=['POST', 'GET']) #product search
def db_product_search():
    if request.method == 'GET':
        school_id = request.args.get('school_id')
        df = pd.read_sql_query(text("""select product_name, product_price from products where school_id=:school_id"""),con=engine, params={'school_id':school_id}) 
        df.set_index('product_name', inplace=True)       
        json_data = df.to_json(orient='columns')
        return json_data

@app.route('/db_house_search', methods=['POST', 'GET']) #house search
def db_house_search():
    if request.method == 'GET':
        school_id = request.args.get('school_id')
        df = pd.read_sql_query(text("""select house_name from house where school_id=:school_id"""),con=engine, params={'school_id':school_id})     
        json_data = df.to_json(orient='columns')
        return json_data
    
@app.route('/db_save_student_invoice', methods=['POST', 'GET']) #save student invoice data
def db_save_student_invoice():
    if request.method == 'POST':
        response=request.get_json()
        output=json.loads(response)
        my_date= datetime.strptime(output['header']['Date'], '%Y-%m-%d')
        bill_no= 'PWPL/'+str(output['session']['school_code'])+'/'+str(my_date.year)+'/'+str(my_date.month)+'/'+str(output['header']['Roll No.'])    
        df = pd.read_sql_query(text("""select id from house where school_id=:school_id and house_name=:house_name"""),con=engine, params={'school_id':output['session']['school_id'], 'house_name':output['header']['House']})     
        house_id = df.iloc[0][0]
        list_of_tuples=[]
        for x in output['products'].keys():
            if ('_size' in x) == False:
                df = pd.read_sql_query(text("""select id from products where school_id=:school_id and product_name=:product_name"""),con=engine, params={'school_id':output['session']['school_id'], 'product_name':x})     
                product_id=df.iloc[0][0]                     
                for y in output['products'].keys():
                    if ('_size' in y) == True and (x in y) == True:
                        size=int(output['products'][y])
                        break    
                list_of_tuples.append(tuple([output['header']['Roll No.'],output['header']['Name'],output['header']['Class'],house_id,product_id,output['products'][x][0],output['products'][x][2],False,
                                             output['header']['Date'], bill_no, output['session']['school_id'],output['session']['user_id'],size]))
    columns=['roll_no','student_name','class','house_id','item_id','item_quantity','total_price','tc_leave','date_of_purchase','bill_no','school_id','user_id','size']
    df=pd.DataFrame(list_of_tuples, columns=columns)
    con1=engine.connect()
    con1.autocommit= True
    df.to_sql('sales', con1, if_exists='append', index=False)
    return jsonify(bill_no)

@app.route('/db_search_student_invoice', methods=['POST', 'GET']) #search student invoice and send the data to regenrate invoice
def db_search_student_invoice():
    if request.method == 'GET':
        inv_no = request.args.get('inv_no')
        date_of_purchase = request.args.get('date_of_purchase')
        query=text("""select student_name, class, roll_no, date_of_purchase, house_name, bill_no, img_url, tc_leave, sum(item_quantity) as item_quantity, sum(total_price) as total_price
                    from sales s 
                    join house h on h.id=s.house_id
                    join schools s1 on s1.id=s.school_id
                    where bill_no=:bill_no and date_of_purchase=:date_of_purchase
                    group by 1,2,3,4,5,6,7,8""")   
        df=pd.read_sql(query, con=engine, params={'bill_no':inv_no, 'date_of_purchase':date_of_purchase})
        json_data={}
        if len(df) == 0:
            return jsonify({"found":False})    
        else:
            if df.loc[0,'tc_leave']==False:
                df.loc[0,'tc_leave']="This Invoice is Marked for TC/Leave as NO"
            else:
                df.loc[0,'tc_leave']="This Invoice is Marked for TC/Leave as YES"
            wa=number_to_word(df.loc[0,'total_price'])
            df.insert(8, 'Word Amount',[wa], True)
            df.loc[0,'total_price']=[format_currency(df.loc[0,'total_price'], 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False)]
            json_data['headers'] = df.to_json(orient='columns')
            json_data['found']= True
            query=text("""select product_name, size, item_quantity, product_price, total_price from sales s
                            join products p on s.item_id=p.id
                            where bill_no=:bill_no and date_of_purchase=:date_of_purchase""")
            df=pd.read_sql(query, con=engine, params={'bill_no':inv_no, 'date_of_purchase':date_of_purchase})
            df['product_price']=df['product_price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
            df['total_price']=df['total_price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
            json_data['products'] = df.to_json(orient='columns')  
        return jsonify(json_data)

@app.route('/db_product_pivot_principal_bill', methods=['POST', 'GET']) #search student invoice and send the data to regenrate invoice
def db_product_pivot_principal_bill():
    if request.method == 'GET':
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        school_id=request.args.get('school_id')
        tc_leave=request.args.get('tc_leave')
        query=text(""" select p.product_name, p.product_price, sum(item_quantity) as item_quantity, sum(total_price) as total_price
                    from sales s
                    join products p on p.id=s.item_id
                    where date_of_purchase BETWEEN :start_date AND :end_date  AND s.school_id=:school_id AND s.tc_leave=:tc_leave
                    group by p.product_name, p.product_price;""")
        df=pd.read_sql(query, con=engine, params={'start_date':start_date, 'end_date':end_date, 'school_id':school_id, 'tc_leave':tc_leave})
        json_data = df.to_json(orient='columns')
        return json_data
        

if __name__ == '__main__':
   app.run(debug = True, host='127.1.1.1', port=8080)