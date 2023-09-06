from pyhive import hive

# Connect to Hive
conn = hive.connect('localhost')
cursor = conn.cursor()
# purchase_id,customer_id, first_name, last_name, country, club, card_type, gender, currency, email, 
# product_id, product_name, product_price, quantity, department_id, department_name, sub_department_id ,
# sub_department_name, purchase_time, online , country_code, total , city


query ="""select count(*)
 from test_3108
 """

cursor.execute(query)

results = cursor.fetchall()
print((results))
#for result in results:
#    print(result)