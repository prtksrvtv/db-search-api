from flask import Flask, request, jsonify, json
from sqlalchemy import create_engine, text
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, date
from babel.numbers import format_currency
from numtoword import number_to_word
from date_format_change import change_date_format 
from flask_cors import CORS
from inventory_update import *
from check_duplicate_invoice import *

load_dotenv()
engine = create_engine(os.environ['DATABASE_URL'])
app = Flask(__name__)
CORS(app)
@app.route('/check', methods=['GET']) #health check 
def check():
    if request.method == 'GET':
        return jsonify({'status': 200})
    
@app.route('/login', methods=['GET'])
def login():
    if request.method == 'GET':
        params_dict=request.args.to_dict()
        df = pd.read_sql_query(text("""select u.id as user_id, u.school_id as school_id, name as user_name, username as user_email, password, school_name, school_code, pic_url from users u join schools s on u.school_id=s.id 
                        where username=:username"""),con=engine, params={'username':params_dict['username']})
        if len(df) == 0:
            result={'status':400, 'message':'Incorrect Email', 'error': True}
        else:
            if df['password'][0] == params_dict['password']:
                df=df.drop('password', axis=1)
                df= df.to_json(orient='records')
                result={'status':200, 'message':'Authenticated', 'error': False, 'data':df}
            else:
                result={'status':400, 'message':'Incorrect Password', 'error': True}
            
    return result 

@app.route('/get_last_invoice_details', methods=['GET'])
def get_last_invoice_details():
    if request.method == 'GET':
        school_id=request.args.get('school_id')
        df = pd.read_sql_query(text("""select bill_no, date_of_purchase from sales where school_id=:school_id order by created_at desc limit 1"""),con=engine, params={'school_id':school_id})
        query = text("""with cte as(
                                    select count(bill_no) as cou
                                    from sales 
                                    where date_of_purchase >= date_trunc('year', now()) and date_of_purchase <= now() and school_id=:school_id
                                    group by date_of_purchase, bill_no )
                                    select sum(cou) from cte
                                    """)
        df1 = pd.read_sql_query(query,con=engine, params={'school_id':school_id})#get count of invoicves
        d=change_date_format(str(df['date_of_purchase'][0]))
        return(jsonify ({'count':int(df1.iloc[0][0]), 'bill_no':df['bill_no'][0], 'date_of_purchase':d}))

@app.route('/db_product_search', methods=['GET']) #product search
def db_product_search():
    if request.method == 'GET':
        school_id = request.args.get('school_id') #getting arguments
        df = pd.read_sql_query(text("""select id, product_name, product_price from products where school_id=:school_id"""),con=engine, params={'school_id':school_id})    
        json_data = df.to_json(orient='records') #converting to json
        #a=df['product_name'].values.tolist()
        return json.dumps(json_data)

@app.route('/db_house_search', methods=['GET']) #house search
def db_house_search():
    if request.method == 'GET':
        school_id = request.args.get('school_id')
        df = pd.read_sql_query(text("""select house_name from house where school_id=:school_id"""),con=engine, params={'school_id':school_id})     
        #json_data = df.to_json(orient='columns')
        a=df['house_name'].values.tolist()
        return json.dumps(a)
    
@app.route('/db_save_student_invoice', methods=['POST']) #save student invoice data
def db_save_student_invoice():
    if request.method == 'POST':
        response=request.get_json()
        check = check_duplicate_invoice(response['header']['bill_no'], response['header']['date_of_purchase'], response['header']['schoolID'])
        if check == 500:
            return jsonify({'response':300})
        elif check == 200:
            df_products= pd.DataFrame.from_dict(response['products'] ,orient='index') #creating the products df
            update = inventory_update(df_products, response['header']['schoolID'])
            if update == 500:
                return jsonify({'response':500})
            if update == 200:
                df_save_details_in_db=df_products #creating another instance
                #getting house id and school code
                df_school = pd.read_sql_query(text("""select h.id from house h join schools s on h.school_id=s.id where h.school_id=:school_id and h.house_name=:house_name"""),con=engine, params={'school_id':response['header']['schoolID'], 'house_name':response['header']['house_name']})     
                house_id_var = df_school.iloc[0][0] #storing house id
                #school_code=df_school.iloc[0][1] #storing school code
                #inv_no= 'PWPL/'+school_code+'/'+str(my_date.year)+'/'+str(my_date.month)+'/'+str(df_header['Roll No.'][0])     #generating the invoice number
                #getting product ids and product names 
                #df = pd.read_sql_query(text("""select id as item_id, product_name as "Product Name" from products where school_id=:school_id"""),con=engine, params={'school_id':response['header']['schoolID']})
                #adding new columns to df
                df_save_details_in_db=df_save_details_in_db.assign(roll_no=response['header']['roll_no'])
                df_save_details_in_db=df_save_details_in_db.assign(student_name=response['header']['student_name'])
                df_save_details_in_db=df_save_details_in_db.assign(student_class=response['header']['class'])
                df_save_details_in_db=df_save_details_in_db.assign(date_of_purchase=response['header']['date_of_purchase'])
                df_save_details_in_db=df_save_details_in_db.assign(house_id=house_id_var)
                df_save_details_in_db=df_save_details_in_db.assign(bill_no=response['header']['bill_no'])
                df_save_details_in_db=df_save_details_in_db.assign(tc_leave=False)
                df_save_details_in_db=df_save_details_in_db.assign(school_id=response['header']['schoolID'])
                df_save_details_in_db=df_save_details_in_db.assign(user_id=response['header']['userID'])
                #adding the product ids by merging the dfs
                #df_save_details_in_db=df_save_details_in_db.merge(df, on='Product Name', how='left')
                df_save_details_in_db.drop(columns={'product_name', 'product_price'}, inplace=True) #dropping some columns
                #renaming columns
                df_save_details_in_db.rename(columns={'id':'item_id','qty':'item_quantity', 'student_class':'class'}, inplace=True)
                #rearranging the columns
                df_save_details_in_db=df_save_details_in_db[['roll_no','student_name','class','house_id','item_id','item_quantity','total_price','tc_leave','date_of_purchase','bill_no','school_id','user_id','size']]
                df_save_details_in_db=df_save_details_in_db.reset_index()
                df_save_details_in_db.drop(columns={'index'}, inplace=True) #dropping some columns
                con1=engine.connect() #creating new connection to save df to db
                con1.autocommit= True
                df_save_details_in_db.to_sql('sales', con1, if_exists='append', index=False) #persisting the df to db
                wa=number_to_word(response['header']['total_price'])
                return jsonify({'word_amount':wa, 'response':200})
            

@app.route('/db_search_student_invoice', methods=['GET']) #search student invoice and send the data to regenrate invoice
def db_search_student_invoice():
    if request.method == 'GET':
        params_dict=request.args.to_dict()
        query=text("""select s.user_id, student_name, class, roll_no, date_of_purchase, house_name, bill_no, s1.school_name, s1.id as school_id, tc_leave, sum(item_quantity) as item_quantity, sum(total_price) as total_price
                    from sales s 
                    join house h on h.id=s.house_id
                    join schools s1 on s1.id=s.school_id
                    where bill_no=:bill_no and date_of_purchase=:date_of_purchase
                    group by 1,2,3,4,5,6,7,8,9,10""")   
        df=pd.read_sql(query, con=engine, params={'bill_no':params_dict['inv_no'], 'date_of_purchase':params_dict['date_of_purchase']}) #creating the header df
        json_data={}
        if len(df) == 0:
            return jsonify({"found":False})     #check if the invoice data is present in db 
        else:
            #if df.loc[0,'tc_leave']==False: #capturing the TC/Leave data from db
             #   df.loc[0,'tc_leave']="This Invoice is Marked for TC/Leave as NO"
            #else:
             #   df.loc[0,'tc_leave']="This Invoice is Marked for TC/Leave as YES"
            wa=number_to_word(df.loc[0,'total_price'])
            df.insert(8, 'Word Amount',[wa], True) #adding column to df
            #df.loc[0,'total_price']=[format_currency(df.loc[0,'total_price'], 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False)] #passing in a list format
            json_data['headers'] = df.to_json(orient='records') #converting to json
            json_data['found']= True
            query=text("""select product_name, size, item_quantity, product_price, total_price from sales s
                            join products p on s.item_id=p.id
                            where bill_no=:bill_no and date_of_purchase=:date_of_purchase""")
            df=pd.read_sql(query, con=engine, params={'bill_no':params_dict['inv_no'], 'date_of_purchase':params_dict['date_of_purchase']}) #creating the product df
            #formatting the currency values
            #df['product_price']=df['product_price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
            #df['total_price']=df['total_price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
            json_data['products'] = df.to_json(orient='records')   #converting to json
        return jsonify(json_data)

@app.route('/db_product_pivot_principal_bill', methods=['GET']) #search student invoice and send the data to regenrate invoice
def db_product_pivot_principal_bill():
    if request.method == 'GET':
        json_data={}
        params_dict=request.args.to_dict()
        query=text(""" select p.product_name, p.product_price, sum(item_quantity) as item_quantity, sum(total_price) as total_price
                    from sales s
                    join products p on p.id=s.item_id
                    where date_of_purchase >=:start_date AND date_of_purchase<=:end_date  AND s.school_id=:school_id AND s.tc_leave=:tc_leave
                    group by p.product_name, p.product_price;""")
        df=pd.read_sql(query, con=engine, params={'start_date':params_dict['start_date'], 'end_date':params_dict['end_date'], 'school_id':params_dict['school_id'], 'tc_leave':params_dict['tc_leave']})
        df.rename(columns={'product_name':'Product Name', 'product_price':'Unit Price', 'item_quantity':'Item Quantity', 'total_price':'Total Price'}, inplace=True)
        df.index = pd.RangeIndex(start=1, stop=1+len(df), step=1)
        item_quantity=str(df['Item Quantity'].sum())
        total_price=str(df['Total Price'].sum())
        word_amount=number_to_word(total_price)
        #total_price=format_currency(total_price, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False)
        #df['Unit Price']=df['Unit Price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
        #df['Total Price']=df['Total Price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
        json_data['products'] = df.to_json(orient='records')
        df=pd.read_sql(text("""select school_name, school_code from schools where id=:school_id"""), con=engine, params={'school_id':params_dict['school_id']})
        school_name=df['school_name'][0]
        school_code=df['school_code'][0]
        bill_date=change_date_format(str(date.today()))
        inv_no=str(abs(hash('PWPL/'+school_code+'/'+str(datetime.now())))) #Invoice No.
        json_data['header']= {'item_quantity':item_quantity, 'total_price':total_price, 'school_name':school_name, 'bill_date':bill_date, 'inv_no':inv_no, 'word_amount':word_amount}
        json_data['found']= True
        return json_data

@app.route('/db_all_house_cover_page', methods=['GET'])
def db_all_house_cover_page():
    if request.method == 'GET':
        json_data={}
        params_dict=request.args.to_dict()
        query=text(""" select h.house_name, count(distinct roll_no) as count_students, sum(item_quantity) as item_quantity, sum(total_price) as total_price
                from sales s
                join house h on s.house_id=h.id
                where date_of_purchase >=:start_date AND date_of_purchase <=:end_date AND s.school_id=:school_id AND s.tc_leave=:tc_leave
                group by h.house_name order by h.house_name;""")
        df=pd.read_sql(query, con=engine, params={'start_date':params_dict['start_date'], 'end_date':params_dict['end_date'], 'school_id':params_dict['school_id'], 'tc_leave':params_dict['tc_leave']})
        df.rename(columns={'house_name':'House Name', 'item_quantity':'Item Quantity', 'total_price':'Total Price', 'count_students':'Count of Students'}, inplace=True)
        df.index = pd.RangeIndex(start=1, stop=1+len(df), step=1)
        item_quantity=str(df['Item Quantity'].sum())
        total_price=str(df['Total Price'].sum())
        word_amount=number_to_word(total_price)
        #total_price=format_currency(total_price, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False)
        #df['Total Price']=df['Total Price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
        json_data['data'] = df.to_json(orient='records')
        df=pd.read_sql(text("""select school_name, school_code from schools where id=:school_id"""), con=engine, params={'school_id':params_dict['school_id']})
        school_name=df['school_name'][0]
        json_data['header']={'item_quantity':item_quantity, 'total_price':total_price, 'word_amount':word_amount, 'school_name':school_name}
        json_data['found']=True
        return json_data         

@app.route('/db_individual_house_cover_page', methods=['GET'])
def db_individual_house_cover_page():
    if request.method == 'GET':
        json_data={}
        params_dict=request.args.to_dict()
        query=text(""" select roll_no, student_name, class, sum(item_quantity) as item_quantity, sum(total_price) as total_price	
                from sales s
                join house h on s.house_id=h.id
                where date_of_purchase >=:start_date AND date_of_purchase <=:end_date AND h.house_name=:house_name AND s.school_id=:school_id AND s.tc_leave=:tc_leave
                group by roll_no, student_name, class 
                order by class,roll_no ;""" )
        df=pd.read_sql(query, con=engine, params={'start_date':params_dict['start_date'], 'end_date':params_dict['end_date'], 'house_name':params_dict['house'], 'school_id':params_dict['school_id'], 'tc_leave':params_dict['tc_leave']})
        df.rename(columns={'roll_no':'Roll No.', 'student_name':'Student Name', 'class':'Class', 'item_quantity':'Item Quantity', 'total_price':'Total Price'}, inplace=True)
        df.index = pd.RangeIndex(start=1, stop=1+len(df), step=1)
        item_quantity=str(df['Item Quantity'].sum())
        total_price=str(df['Total Price'].sum())
        word_amount=number_to_word(total_price)
        #total_price=format_currency(total_price, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False)
        #df['Total Price']=df['Total Price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
        json_data['data'] = df.to_json(orient='records')
        df=pd.read_sql(text("""select school_name, school_code from schools where id=:school_id"""), con=engine, params={'school_id':params_dict['school_id']})
        school_name=df['school_name'][0]
        json_data['header']={'item_quantity':item_quantity, 'total_price':total_price, 'word_amount':word_amount, 'school_name':school_name, 'house_name':params_dict['house']}
        json_data['found']=True
        return json_data

@app.route('/db_check_student_invoice_present', methods=['GET'])
def db_check_student_invoice_present():
    if request.method == 'GET':
        params_dict=request.args.to_dict()
        query= text(""" select count(distinct bill_no) from sales where bill_no=:inv_no and date_of_purchase=:date_of_purchase""")
        df=pd.read_sql(query, con=engine, params={'inv_no':params_dict['inv_no'], 'date_of_purchase':params_dict['date_of_purchase']})
        if df['count'][0] == 0:
            return jsonify({'found':False})
        else:
            return jsonify({'found':True})

@app.route('/db_delete_student_invoice', methods=['GET'])
def db_delete_student_invoice():
    if request.method == 'GET':
        params_dict=request.args.to_dict()
        query= text("""delete from sales
                where bill_no=:inv_no and date_of_purchase=:date_of_purchase;""")
        with engine.connect() as c:
            c.execute(query, {'inv_no':params_dict['inv_no'], 'date_of_purchase':params_dict['date_of_purchase']})
            c.commit()
        result="INVOICE NO. "+params_dict['inv_no']+"DELETED FROM DATABASE"
        result={'status':200, 'message':'Invoice Deleted from Database', 'error': False}
        return jsonify(result)

@app.route('/db_change_student_invoice_tc_leave_status', methods=['GET'])
def db_change_student_invoice_tc_leave_status():
    if request.method == 'GET':
        params_dict=request.args.to_dict()
        if params_dict['tc_leave'].lower() == 'true':
            params_dict['tc_leave']=False
        elif params_dict['tc_leave'].lower() == 'false':
            params_dict['tc_leave']=True
        query= text("""update sales
                        set tc_leave=:tc_leave
                        where bill_no=:inv_no and date_of_purchase=:date_of_purchase ;""")
        with engine.connect() as c:
            c.execute(query, {'tc_leave':params_dict['tc_leave'], 'inv_no':params_dict['inv_no'], 'date_of_purchase':params_dict['date_of_purchase']})
            c.commit()
        result={'status':200, 'message':'Invoice Status updated in Database', 'error': False}
        return jsonify(result)

@app.route('/db_stock_input', methods=['POST'])
def db_update_inventory():
    if request.method == 'POST':
        try:
            #get json from client
            input=request.get_json()
            #fe means front end
            df_stock_fe = pd.DataFrame.from_dict(input['products'], orient='index')
            for i,row in df_stock_fe.iterrows():
                row['entry']=row['entry'].split(',')
                df_list=[]
                flag=True
                for x in row['entry']:
                    temp=x.split(':')
                    temp[0]=int(temp[0])
                    temp[1]=int(temp[1])
                    df_list.append(temp)
                d=pd.DataFrame(df_list, columns=['size','quantity'])
                query=text(""" select item_id,stock_present from stock s 
                    join products p on s.item_id=p.id and s.school_id=p.school_id
                    where s.school_id=:school_id and p.product_name=:product_name""")
                #creating dataframe of current products in stock
                df=pd.read_sql_query(query, con=engine, params={'school_id':input['school_id'], 'product_name':row['product_name']})
                if len(df) == 0:
                    if int(input['type']) == 1:
                        d=d.to_json(orient='index')
                        query=text("""insert into stock (item_id, school_id, stock_present)
                                        values (:item_id, :school_id, :stock_present)""") 
                        with engine.connect() as c:
                            c.execute(query, {'item_id':row['id'], 'school_id':input['school_id'], 'stock_present':d})
                            c.commit()
                    if input['type'] == 2:
                        flag = False
                        continue
                else:
                    stock_present_df=pd.DataFrame.from_dict(df['stock_present'][0], orient='index').set_index('size')
                    for j,stock in d.iterrows(): #checking and updation the stock present dataframe
                        if stock['size'] in stock_present_df.index : 
                            if int(input['type']) == 1:   
                                stock_present_df['quantity'][stock['size']] = stock_present_df['quantity'][stock['size']] + stock['quantity']
                            if int(input['type']) == 2:
                                stock_present_df['quantity'][stock['size']] = stock['quantity']                    
                        else:
                            if int(input['type']) == 1:
                                stock_present_df.loc[stock['size']] = [stock['quantity']]
                            if int(input['type']) == 2:
                                flag=False
                                continue
                    stock_present_df.reset_index(inplace=True)
                    stock_present_df=stock_present_df.to_json(orient='index') #converting the updated df to json 
                    query=text("""update stock
                            set stock_present=:stock 
                            where item_id=:item_id and school_id=:school_id""")
                    #save the updated df to the db
                    with engine.connect() as c:
                        c.execute(query, {'stock':stock_present_df, 'item_id':row['id'], 'school_id':input['school_id']})
                        c.commit()
            if flag:
                response = 200 #success
            else:
                response = 100 #some were wrong entries hence ignored
            return jsonify({'response': response})
        except:
            return jsonify({'response': 500}) #handling exceptions in case of wrong inputs
    
@app.route('/db_view_inventory', methods=['GET'])
def db_view_inventory():
    if request.method == 'GET':
        school_id = request.args.get('school_id')
        query=text("""select product_name,stock_present from stock s 
                        join products p on s.item_id=p.id and s.school_id=p.school_id
                        where s.school_id=:school_id""")
        df=pd.read_sql_query(query, con=engine, params={'school_id':school_id}, index_col='product_name')
        df=df.to_json(orient='index')
        return df

#@app.route('/db_update_inventory', methods=['POST'])
#def db_update_inventory():
#    if request.method == 'POST':
#        input = request.get_json()
#       #fe means front end
#        df_stock_fe = pd.DataFrame.from_dict(input['products'], orient='index')
#        
#        for x in input['products']:
#            query=text("""select item_id,stock_present from stock s 
#                        join products p on s.item_id=p.id and s.school_id=p.school_id
#                        where s.school_id=:school_id and product_name=:product_name""")
#            df=pd.read_sql_query(query, con=engine, params={'school_id':input['school_id'], 'product_name':x})
#            if len(df) == 0:
#                return jsonify({'response':'Product does not exist in inventory'})
#        else:
#            stock=pd.DataFrame(df.iloc[0,1]).transpose()
#            stock.set_index('size', inplace=True)
#            for y in products[x]:
#                temp=y.split(":")                    
#                if (int(temp[0]) in stock.index) is False:
#                    stock['quantity'][int(temp[0])]=int(temp[1])
#                stock.reset_index(inplace=True)
#                stock=stock.to_json(orient='index')
#                query=text("""update stock
#                          set stock_present=:stock 
#                           where item_id=:item_id and school_id=:school_id""")
#                with engine.connect() as c:
#                    c.execute(query, {'stock':stock, 'item_id':str(df.iloc[0,0]), 'school_id':str(input['school_id'])})
#                    c.commit()    
#        return jsonify({'response':True})



@app.route('/db_raashan_products_search', methods=['GET'])
def db_raashan_products_search():
    if request.method == 'GET':
        tender = request.args.get('tender')
        df = pd.read_sql_query(text("""select tender_s_no,item_name,item_unit,rate,gst_amount
                                     from raashan_products where tender_number=:tender order by tender_number,tender_s_no"""),con=engine, params={'tender':tender})       
        json_data = df.to_json(orient='records')
        return json_data

@app.route('/db_save_raashan_bill_details', methods=['POST'])
def db_save_raashan_bill_details():
    if request.method == 'POST':
        input = request.get_json()
        header=input['header']
        products=pd.DataFrame.from_dict(input['products'], orient='index')
        query=text("""select id, item_name from raashan_products where tender_number=:tender""")
        df=pd.read_sql_query(query, con=engine, params={'tender':header['tender']})
        products=products.assign(invoice_no=header['invoice_no'])
        products=products.assign(start_date=header['start_date'])
        products=products.assign(end_date=header['end_date'])
        products=products.assign(inv_date=header['inv_date'])
        products=products.assign(tender_no=header['tender'])
        products=products.merge(df, on='item_name', how='left')
        products.rename(columns={'item_quantity':'quantity', 'id':'product_id'}, inplace=True)
        products.drop(columns={'tender_s_no', 'item_name', 'item_unit', 'rate', 'gst_amount', }, inplace=True)
        products=products[['invoice_no','product_id','tender_no', 'quantity','start_date', 'end_date', 'total_price', 'inv_date']]
        con1=engine.connect()
        con1.autocommit= True
        products.to_sql('raashan_sales', con1, if_exists='append', index=False)
        wa=number_to_word(header['total_price'])
    return jsonify({'word_amount':wa})

if __name__ == '__main__':
   app.run(debug = True, host='127.1.1.1', port=8080) #for local dev
   #app.run() #cloud run
