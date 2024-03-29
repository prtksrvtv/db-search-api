from flask import Flask, request, jsonify, json
from sqlalchemy import create_engine, text
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, date
from babel.numbers import format_currency
from numtoword import number_to_word
from date_format_change import change_date_format 

load_dotenv()
engine = create_engine(os.environ['DATABASE_URL'])
app = Flask(__name__)

@app.route('/check', methods=['GET']) #health check 
def check():
    if request.method == 'GET':
        return jsonify({'result':'Service is live!'})


@app.route('/db_product_search', methods=['GET']) #product search
def db_product_search():
    if request.method == 'GET':
        school_id = request.args.get('school_id') #getting arguments
        df = pd.read_sql_query(text("""select product_name, product_price from products where school_id=:school_id"""),con=engine, params={'school_id':school_id})       
        json_data = df.to_json(orient='columns') #converting to json
        return json_data

@app.route('/db_house_search', methods=['GET']) #house search
def db_house_search():
    if request.method == 'GET':
        school_id = request.args.get('school_id')
        df = pd.read_sql_query(text("""select house_name from house where school_id=:school_id"""),con=engine, params={'school_id':school_id})     
        json_data = df.to_json(orient='columns')
        return json_data
    
@app.route('/db_save_student_invoice', methods=['POST']) #save student invoice data
def db_save_student_invoice():
    if request.method == 'POST':
        response=request.get_json()
        #desearlizing JSON
        output=json.loads(response) 
        df_header=json.loads(output['header'])
        #creating header df
        df_header=pd.DataFrame(df_header)
        df_products=json.loads(output['products'])
        #creating products df
        df_products=pd.DataFrame(df_products, columns=df_products.keys())
        df_save_details_in_db=df_products #creating another instance
        my_date= datetime.strptime(df_header['Date'][0], '%Y-%m-%d')
        #getting house id and school code
        df_school = pd.read_sql_query(text("""select h.id, s.school_code from house h join schools s on h.school_id=s.id where h.school_id=:school_id and h.house_name=:house_name"""),con=engine, params={'school_id':output['school_id'], 'house_name':df_header['House'][0]})     
        house_id_var = df_school.iloc[0][0] #storing house id
        school_code=df_school.iloc[0][1] #storing school code
        inv_no= 'PWPL/'+school_code+'/'+str(my_date.year)+'/'+str(my_date.month)+'/'+str(df_header['Roll No.'][0])     #generating the invoice number
        #getting product ids and product names 
        df = pd.read_sql_query(text("""select id as item_id, product_name as "Product Name" from products where school_id=:school_id"""),con=engine, params={'school_id':output['school_id']})
        #adding new columns to df
        df_save_details_in_db=df_save_details_in_db.assign(roll_no=df_header['Roll No.'][0])
        df_save_details_in_db=df_save_details_in_db.assign(student_name=df_header['Name'][0])
        df_save_details_in_db=df_save_details_in_db.assign(student_class=df_header['Class'][0])
        df_save_details_in_db=df_save_details_in_db.assign(date_of_purchase=df_header['Date'][0])
        df_save_details_in_db=df_save_details_in_db.assign(house_id=house_id_var)
        df_save_details_in_db=df_save_details_in_db.assign(bill_no=inv_no)
        df_save_details_in_db=df_save_details_in_db.assign(tc_leave=False)
        df_save_details_in_db=df_save_details_in_db.assign(school_id=output['school_id'])
        df_save_details_in_db=df_save_details_in_db.assign(user_id=output['user_id'])
        #adding the product ids by merging the dfs
        df_save_details_in_db=df_save_details_in_db.merge(df, on='Product Name', how='left')
        df_save_details_in_db.drop(columns={'Product Name', 'Unit Price'}, inplace=True) #dropping some columns
        #renaming columns
        df_save_details_in_db.rename(columns={'Product Size':'size', 'Quantity':'item_quantity', 'Total Price':'total_price', 'student_class':'class'}, inplace=True)
        #rearranging the columns
        df_save_details_in_db=df_save_details_in_db[['roll_no','student_name','class','house_id','item_id','item_quantity','total_price','tc_leave','date_of_purchase','bill_no','school_id','user_id','size']]
        con1=engine.connect() #creating new connection to save df to db
        con1.autocommit= True
        df_save_details_in_db.to_sql('sales', con1, if_exists='append', index=False) #persisting the df to db
        return jsonify(inv_no)

@app.route('/db_search_student_invoice', methods=['GET']) #search student invoice and send the data to regenrate invoice
def db_search_student_invoice():
    if request.method == 'GET':
        params_dict=request.args.to_dict()
        query=text("""select student_name, class, roll_no, date_of_purchase, house_name, bill_no, img_url, tc_leave, sum(item_quantity) as item_quantity, sum(total_price) as total_price
                    from sales s 
                    join house h on h.id=s.house_id
                    join schools s1 on s1.id=s.school_id
                    where bill_no=:bill_no and date_of_purchase=:date_of_purchase
                    group by 1,2,3,4,5,6,7,8""")   
        df=pd.read_sql(query, con=engine, params={'bill_no':params_dict['inv_no'], 'date_of_purchase':params_dict['date_of_purchase']}) #creating the header df
        json_data={}
        if len(df) == 0:
            return jsonify({"found":False})     #check if the invoice data is present in db 
        else:
            if df.loc[0,'tc_leave']==False: #capturing the TC/Leave data from db
                df.loc[0,'tc_leave']="This Invoice is Marked for TC/Leave as NO"
            else:
                df.loc[0,'tc_leave']="This Invoice is Marked for TC/Leave as YES"
            wa=number_to_word(df.loc[0,'total_price'])
            df.insert(8, 'Word Amount',[wa], True) #adding column to df
            df.loc[0,'total_price']=[format_currency(df.loc[0,'total_price'], 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False)] #passing in a list format
            json_data['headers'] = df.to_json(orient='columns') #converting to json
            json_data['found']= True
            query=text("""select product_name, size, item_quantity, product_price, total_price from sales s
                            join products p on s.item_id=p.id
                            where bill_no=:bill_no and date_of_purchase=:date_of_purchase""")
            df=pd.read_sql(query, con=engine, params={'bill_no':params_dict['inv_no'], 'date_of_purchase':params_dict['date_of_purchase']}) #creating the product df
            #formatting the currency values
            df['product_price']=df['product_price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
            df['total_price']=df['total_price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
            json_data['products'] = df.to_json(orient='columns')   #converting to json
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
        total_price=df['Total Price'].sum()
        word_amount=number_to_word(total_price)
        total_price=format_currency(total_price, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False)
        df['Unit Price']=df['Unit Price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
        df['Total Price']=df['Total Price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
        json_data['products'] = df.to_json(orient='columns')
        df=pd.read_sql(text("""select school_name, school_code from schools where id=:school_id"""), con=engine, params={'school_id':params_dict['school_id']})
        school_name=df['school_name'][0]
        school_code=df['school_code'][0]
        bill_date=change_date_format(str(date.today()))
        inv_no=str(abs(hash('PWPL/'+school_code+'/'+str(datetime.now())))) #Invoice No.
        json_data['header']= {'item_quantity':item_quantity, 'total_price':total_price, 'school_name':school_name, 'bill_date':bill_date, 'inv_no':inv_no, 'word_amount':word_amount}
        return json_data

@app.route('/db_all_house_cover_page', methods=['GET'])
def db_all_house_cover_page():
    if request.method == 'GET':
        json_data={}
        params_dict=request.args.to_dict()
        query=text(""" select h.house_name, sum(item_quantity) as item_quantity, sum(total_price) as total_price
                from sales s
                join house h on s.house_id=h.id
                where date_of_purchase >=:start_date AND date_of_purchase <=:end_date AND s.school_id=:school_id AND s.tc_leave=:tc_leave
                group by h.house_name order by h.house_name;""")
        df=pd.read_sql(query, con=engine, params={'start_date':params_dict['start_date'], 'end_date':params_dict['end_date'], 'school_id':params_dict['school_id'], 'tc_leave':params_dict['tc_leave']})
        df.rename(columns={'house_name':'House Name', 'item_quantity':'Item Quantity', 'total_price':'Total Price'}, inplace=True)
        df.index = pd.RangeIndex(start=1, stop=1+len(df), step=1)
        item_quantity=str(df['Item Quantity'].sum())
        total_price=df['Total Price'].sum()
        word_amount=number_to_word(total_price)
        total_price=format_currency(total_price, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False)
        df['Total Price']=df['Total Price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
        json_data['data'] = df.to_json(orient='columns')
        json_data['header']={'item_quantity':item_quantity, 'total_price':total_price, 'word_amount':word_amount}
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
        total_price=df['Total Price'].sum()
        word_amount=number_to_word(total_price)
        total_price=format_currency(total_price, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False)
        df['Total Price']=df['Total Price'].apply(lambda x:format_currency(x, 'INR', format=u'#,##0\xa0¤', locale='en_IN', currency_digits=False))
        json_data['data'] = df.to_json(orient='columns')
        json_data['header']={'item_quantity':item_quantity, 'total_price':total_price, 'word_amount':word_amount}
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
        return jsonify(result)

@app.route('/db_change_student_invoice_tc_leave_status', methods=['GET'])
def db_change_student_invoice_tc_leave_status():
    if request.method == 'GET':
        params_dict=request.args.to_dict()
        if params_dict['tc_leave'].lower() == 'true':
            params_dict['tc_leave']=True
            flag="YES"
        elif params_dict['tc_leave'].lower() == 'false':
            params_dict['tc_leave']=False
            flag="NO"
        query= text("""update sales
                        set tc_leave=:tc_leave
                        where bill_no=:inv_no and date_of_purchase=:date_of_purchase ;""")
        with engine.connect() as c:
            c.execute(query, {'tc_leave':params_dict['tc_leave'], 'inv_no':params_dict['inv_no'], 'date_of_purchase':params_dict['date_of_purchase']})
            c.commit()
        result="INVOICE NO. "+params_dict['inv_no']+" MARKED AS TC/LEAVE "+flag+ " IN THE DATABASE"
        return jsonify(result)

@app.route('/db_stock_input', methods=['POST'])
def db_stock_input():
    if request.method == 'POST':
        #get json from client
        input=request.get_json()
        #deserealizing json
        input=json.loads(input)
        #creating product dictionary from client
        dict_products=input['products']       
        #iterating over product dictionary
        for x in dict_products:
            #query to extract data from stock table
            query=text(""" select item_id,stock_present from stock s 
                   join products p on s.item_id=p.id and s.school_id=p.school_id
                   where s.school_id=:school_id and p.product_name=:product_name""")
            #creating dataframe of current products in stock
            df=pd.read_sql_query(query, con=engine, params={'school_id':input['school_id'], 'product_name':x})
            if len(df) == 0:
                df_list=[] #empty list
                for y in dict_products[x]:
                    temp=y.split(":") #splitting pairs
                    temp[0]=int(temp[0]) #converting to int
                    temp[1]=int(temp[1])
                    df_list.append(temp) #appending the pairs to list
                #creating temporary df and converting to json to hold the new stock entered by client
                d=pd.DataFrame(df_list, columns=['size','quantity']).to_json(orient='index') 
                query=text("""select id from products where product_name=:product_name and school_id=:school_id""")
                temp=pd.read_sql(query, con=engine, params={'product_name':x, 'school_id':input['school_id']})    #getting product id from product table
                query=text("""insert into stock (item_id, school_id, stock_present)
                                values (:item_id, :school_id, :stock_present)""") 
                with engine.connect() as c:
                    c.execute(query, {'item_id':str(temp.iloc[0,0]), 'school_id':str(input['school_id']), 'stock_present':d})
                    c.commit()
            else:
                stock=pd.DataFrame(df.iloc[0,1]).transpose()
                stock.set_index('size', inplace=True)
                for y in dict_products[x]: #iterating over different size:quantity in product dictionary
                    temp=y.split(":") #splitting each pair
                    if (int(temp[0]) in stock.index) is True: #checking if size is there in stock
                        stock['quantity'][int(temp[0])] =stock.loc[int(temp[0]),'quantity']+int(temp[1]) #adding stock
                    else:
                        stock = stock._append(pd.Series({'quantity':int(temp[1])}, name=int(temp[0]))) #adding new row of size in stock present df
                stock.reset_index(inplace=True)
                stock=stock.to_json(orient='index') #converting to json
                query=text("""update stock
                           set stock_present=:stock 
                           where item_id=:item_id and school_id=:school_id""")
                with engine.connect() as c:
                    c.execute(query, {'stock':stock, 'item_id':str(df.iloc[0,0]), 'school_id':str(input['school_id'])})
                    c.commit()
        return jsonify({'Message':'Stock input Successful'})
    
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

@app.route('/db_update_inventory', methods=['POST'])
def db_update_inventory():
    if request.method == 'POST':
        input = request.get_json()
        input=json.loads(input)
        products=input['products']
        for x in products:
            query=text("""select item_id,stock_present from stock s 
                        join products p on s.item_id=p.id and s.school_id=p.school_id
                        where s.school_id=:school_id and product_name=:product_name""")
            df=pd.read_sql_query(query, con=engine, params={'school_id':input['school_id'], 'product_name':x})
            if len(df) == 0:
                return jsonify({'response':False})
            else:
                stock=pd.DataFrame(df.iloc[0,1]).transpose()
                stock.set_index('size', inplace=True)
                for y in products[x]:
                    temp=y.split(":")                    
                    if (int(temp[0]) in stock.index) is False:
                        return jsonify({'response':False})
                    else:
                        stock['quantity'][int(temp[0])]=int(temp[1])
                stock.reset_index(inplace=True)
                stock=stock.to_json(orient='index')
                query=text("""update stock
                           set stock_present=:stock
                           where item_id=:item_id and school_id=:school_id""")
                with engine.connect() as c:
                    c.execute(query, {'stock':stock, 'item_id':str(df.iloc[0,0]), 'school_id':str(input['school_id'])})
                    c.commit()    
        return jsonify({'response':True})

@app.route('/db_raashan_products_search', methods=['GET'])
def db_raashan_products_search():
    if request.method == 'GET':
        tender = request.args.get('tender')
        df = pd.read_sql_query(text("""select tender_s_no,item_name,item_unit,rate,gst_amount
                                     from raashan_products where tender_number=:tender order by tender_number,tender_s_no"""),con=engine, params={'tender':tender})       
        json_data = df.to_json(orient='columns')
        return json_data

@app.route('/db_save_raashan_bill_details', methods=['POST'])
def db_save_raashan_bill_details():
    if request.method == 'POST':
        input = request.get_json()
        header=json.loads(input['header'])
        products=pd.DataFrame(json.loads(input['products']))
        query=text(""" select id, item_name as "Item Name" from raashan_products where tender_number=:tender""")
        df=pd.read_sql_query(query, con=engine, params={'tender':header['tender']})
        products=products.assign(invoice_no=header['Invoice No.'])
        products=products.assign(start_date=header['start_date'])
        products=products.assign(end_date=header['end_date'])
        products=products.assign(inv_date=header['inv_date'])
        products=products.assign(tender_no=header['tender'])
        products=products.merge(df, on='Item Name', how='left')
        products.rename(columns={'Total Quantity':'quantity', 'id':'product_id', 'Total Price':'total_price'}, inplace=True)
        products.drop(columns={'Tender S. No.', 'Item Name', 'Unit', 'Rate per Unit', 'GST Amount per Unit', }, inplace=True)
        products=products[['invoice_no','product_id','tender_no', 'quantity','start_date', 'end_date', 'total_price', 'inv_date']]
        con1=engine.connect()
        con1.autocommit= True
        products.to_sql('raashan_sales', con1, if_exists='append', index=False)
    return jsonify({'response':"Success"})

if __name__ == '__main__':
   #app.run(debug = True, host='127.1.1.1', port=8080) #for local dev
   app.run(debug = False) #cloud run
