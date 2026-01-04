import csv
import sqlite3
from pathlib import Path

db = Path("db/sales.sqlite")
if db.exists():
    db.unlink()

def load_table(file_name, fields):
    table = []
    with open(file_name, encoding = "utf8") as f:
        reader = csv.DictReader(f, fieldnames=fields)
        for line in reader:
            table.append(line)
    return table

transactions = load_table("db/sales_transactions.csv", ("ID", "product", "customer", "quantity", "order_date") )
customers = load_table("db/customer_dim.csv", ("ID", "address", "age", "start_date", "end_date", "current"))
products = load_table("db/product_dim.csv", ("ID", "name", "price", "start_date", "end_date", "current"))

connection = sqlite3.connect("db/sales.sqlite")
connection.row_factory = sqlite3.Row
cursor = connection.cursor()
sql_create_table_customers = """create table if not exists customers (
    ID integer integer,
    address text,
    age integer,
    start_date date,
    end_date date,
    current text
)"""
cursor.execute(sql_create_table_customers)
insert_statement_customers = """insert into customers(ID, address, age, start_date, end_date, current)
values(:ID, :address, :age, :start_date, :end_date, :current);"""
cursor.executemany(insert_statement_customers, customers)
connection.commit()

sql_create_table_products = """create table if not exists products (
    ID integer integer,
    name text,
    price number,
    start_date date,
    end_date date,
    current text
)"""
cursor.execute(sql_create_table_products)
insert_statement_products = """insert into products(ID, name, price, start_date, end_date, current)
values(:ID, :name, :price, :start_date, :end_date, :current);"""
cursor.executemany(insert_statement_products, products)
connection.commit()

sql_create_table_transactions = """create table if not exists transactions (
    ID integer integer,
    product integer,
    customer integer,
    quantity integer,
    order_date date,
    foreign key(product) references products(ID),
    foreign key(customer) references customers(ID)
)"""
cursor.execute(sql_create_table_transactions)
insert_statement_transactions = """insert into transactions(ID, product, customer, quantity, order_date)
values(:ID, :product, :customer, :quantity, :order_date);"""
cursor.executemany(insert_statement_transactions, transactions)
connection.commit()

input("waiting before inserting new clients...")

new_clients = [
    {"ID": 1001, "address": "Musterstrasse 1, 8000 Zürich", "age": 30, "start_date": "2026-04-01", "end_date": "", "current": "Y"},
    {"ID": 1002, "address": "Beispielweg 5, 3000 Bern", "age": 45, "start_date": "2026-04-01", "end_date": "", "current": "Y"}
]

cursor.executemany(insert_statement_customers, new_clients)
connection.commit()

new_sales = [
    # 1001 bought 2 flatscreen TVs (product 250)
    {"ID": 5001, "product": 250, "customer": 1001, "quantity": 2, "order_date": "2026-04-02"},
    # 1002 bought 1 4-pack AA batteries (product 101)
    {"ID": 5002, "product": 406, "customer": 1002, "quantity": 1, "order_date": "2026-04-02"}
]

cursor.executemany(insert_statement_transactions, new_sales)
connection.commit()

input("waiting before removing client 1002...")
cursor.execute("delete from customers where ID = 1002;")
connection.commit()

yn = input("Do you want to see orders to a specific address? (y/n) ").lower()
if yn == "y":
    address_input = input("Enter part of the address: ")
    query = """select t.ID as transaction_id, c.address as customer_address, p.name as product_name, t.quantity, t.order_date
    from transactions t
    join customers c on t.customer = c.ID
    join products p on t.product = p.ID
    where c.address like ?;"""
    result = cursor.execute(query, (f"%{address_input}%",))
    print(f"Orders for addresses containing '{address_input}':")
    for row in result.fetchall():
        print("    Transaction ID:", row['transaction_id'], 
              "| Address:", row['customer_address'], 
              "| Product:", row['product_name'], 
              "| Quantity:", row['quantity'], 
              "| Order Date:", row['order_date'])
    
input("waiting before running data analysis...")

# =================================================================
print("\nCustomers age 83, living in Atlanta:")
query = """select * from customers
where age = 83 and address like '%Atlanta%';"""

result = cursor.execute(query)
for row in result.fetchall():
    print(f"    Customer ID: {row['ID']}, Address: {row['address']}, Age: {row['age']}")

# =================================================================
print("\nNumber of transactions per product:")
query = """select p.name as product_name, sum(t.quantity) as total_quantity
from products p
join transactions t on p.ID = t.product
group by p.ID
order by total_quantity desc;"""

result = cursor.execute(query)
for row in result.fetchall():
    print("    Product", row['product_name'], "sold", row['total_quantity'], "units")

# =================================================================
print("\nFirst 10 highest spending customers:")
query = """select c.ID as customer_id, sum(p.price * t.quantity) as total_spent
from customers c
join transactions t on c.ID = t.customer
join products p on t.product = p.ID and p.current = "Y"
group by c.ID
order by total_spent desc
limit 10;"""

result = cursor.execute(query)
for row in result.fetchall():
    print("    Customer", row['customer_id'], "spent total of", row['total_spent'])

# =================================================================
print("\n5 customers with the most orders:")
query = """select c.ID as customer_id, count(t.ID) as order_count
from customers c
left join transactions t on c.ID = t.customer
group by c.ID
order by order_count desc
limit 5;"""

result = cursor.execute(query)
for row in result.fetchall():
    print("    Customer", row['customer_id'], "made", row['order_count'], "orders")

# =================================================================
print("\nFirst 5 customers added:")
query = """select * from customers
order by start_date asc
limit 5;"""

result = cursor.execute(query)
for row in result.fetchall():
    print("    ", row['ID'], row['address'], row['start_date'])

# =================================================================
print("\nAverage age of customers:")
query = """select avg(age) as average_age from customers;"""
result = cursor.execute(query)
for row in result.fetchall():
    print("    Average age is", row['average_age'])

connection.close()