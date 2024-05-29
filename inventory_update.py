from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
import pandas as pd

load_dotenv()
engine = create_engine(os.environ['DATABASE_URL'])

def inventory_update(df_products, school_id):
    flag=True
    #checking if all the stocks are there
    for i,row in df_products.iterrows():
        query=text(""" select item_id,stock_present from stock s 
                        join products p on s.item_id=p.id and s.school_id=p.school_id
                        where s.school_id=:school_id and p.product_name=:product_name""")
        #creating dataframe of current products in stock
        df=pd.read_sql_query(query, con=engine, params={'school_id':school_id, 'product_name':row['product_name']})
        if len(df) == 0:
            flag = False
            break    
        else:
            stock_present_df=pd.DataFrame.from_dict(df['stock_present'][0], orient='index').set_index('size')
            #checking and updating the stock present dataframe
            if int(row['size']) in stock_present_df.index :
                continue            
            else:
                flag = False
                break       
    if flag:      
        for i,row in df_products.iterrows():
            query=text(""" select item_id,stock_present from stock s 
                            join products p on s.item_id=p.id and s.school_id=p.school_id
                            where s.school_id=:school_id and p.product_name=:product_name""")
            #creating dataframe of current products in stock
            df=pd.read_sql_query(query, con=engine, params={'school_id':school_id, 'product_name':row['product_name']})
            stock_present_df=pd.DataFrame.from_dict(df['stock_present'][0], orient='index').set_index('size')
            #checking and updating the stock present dataframe
            stock_present_df['quantity'][int(row['size'])] = stock_present_df['quantity'][int(row['size'])] - int(row['item_quantity'])              
            stock_present_df.reset_index(inplace=True)
            stock_present_df=stock_present_df.to_json(orient='index') #converting the updated df to json 
            query=text("""update stock
                            set stock_present=:stock 
                            where item_id=:item_id and school_id=:school_id""")
            #save the updated df to the db
            with engine.connect() as c:
                c.execute(query, {'stock':stock_present_df, 'item_id':row['id'], 'school_id':school_id })
                c.commit()
        return 200
    else:
        return 500
    