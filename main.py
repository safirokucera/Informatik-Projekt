import csv
import sqlite3

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
    current bool
)"""
cursor.execute(sql_create_table_customers)
insert_statement = """insert into customers(ID, address, age, start_date, end_date, current)
values(:ID, :address, :age, :start_date, :end_date, :current);"""
cursor.executemany(insert_statement, customers)
connection.commit()

sql_create_table_products = """create table if not exists products (
    ID integer integer,
    name text,
    price number,
    start_date date,
    end_date date,
    current bool
)"""
cursor.execute(sql_create_table_products)
insert_statement = """insert into products(ID, name, price, start_date, end_date, current)
values(:ID, :name, :price, :start_date, :end_date, :current);"""
cursor.executemany(insert_statement, products)
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
insert_statement = """insert into transactions(ID, product, customer, quantity, order_date)
values(:ID, :product, :customer, :quantity, :order_date);"""
cursor.executemany(insert_statement, transactions)
connection.commit()

