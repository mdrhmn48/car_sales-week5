import pandas as pd
import numpy as np
import sqlite3 

self.self.df = pd.read_csv("car_sales_table.csv", index_col="StockID")

class car_sales():
    def __init__(self) -> None:
        pass

    #1. changing vehicle name to Sedan
    def replace_VehicleType(old_name, new_name):
        self.df["VehicleType"] = self.df["VehicleType"].replace(old_name, new_name)
        return self.df

    self.df = replace_VehicleType("Saloon", "Sedan")
    self.df.sample(5)

    #2.most expensive car
    def most_expensive_car():
        return self.df[self.df["CostPrice"]==self.df["CostPrice"].max()]
    most_expensive_car()

    #3.getting the cheapest car
    def cheapest_car():
        return self.df[self.df["CostPrice"]==self.df["CostPrice"].min()]
    cheapest_car()

    #4. getting the quanity and average price of each model by passing in model name
    def model_Quantity_Ave(Model):
        grouped = self.df.groupby("Make").aggregate({"Model": "count", "CostPrice":"mean"}).round(2).sort_values("Model", ascending=False)
        return grouped[Model]
    model_Quantity_Ave("Camargue")

    #5.average price of each make/company of the car
    def make_avg():
        make_avg = self.df.groupby("Make").aggregate({"CostPrice": "mean"})
        return make_avg

    #6. Miniumum and Maximum cost price for each car:
    def low_high_cost():
        low_high_cost = self.df.groupby("Make").aggregate({"CostPrice": ["min", "max"]})
        return low_high_cost

    #7. Get the highest mileage car
    def highest_mileage_car():
        return self.df[self.df["Mileage"]==self.df["Mileage"].max()]
    highest_mileage_car()

    #8. Get the lowest mileage car
    def least_mileage_car():
        return self.df[self.df["Mileage"]==self.df["Mileage"].min()]
    least_mileage_car()

    #9. adding a rows to database
    def adding_row():
        dict = {'Make': 'Tesla', 'Model': 'Y', 'ColorID': 1, 'VehicleType': 'Sedan', 'CostPrice':20000, 'SpareParts': 500,
            'LaborCost': 200, 'Registration_Date':'4/5/2023', 'Mileage': 5000, 'PurchaseDate': '4/5/2023'}
        self.df2 = pd.DataFrame([dict])
        self.df = pd.concat([self.df,self.df2], ignore_index = True) 
        self.df = self.df.reset_index()
        return self.df
    #10. getting the quanity and average price of each model by passing in model name
    def model_Quantity_Ave(Model):
        grouped = self.df.groupby("Make").aggregate({"Model": "count", "LaborCost":"mean"}).round(2).sort_values("Model", ascending=False)
        return grouped[Model]
    model_Quantity_Ave("Camargue")


db_name = "MyDB.db"
conn = sqlite3.connect(db_name)
cursor = conn.cursor()

self.df.to_sql("car_sales_table", conn, if_exists="replace", index=False)


query = """
Select * from car_sales_table
"""
# conn.execute(query)
pd.read_sql(query,conn)

def create_table(table="my_cars"):
# Drop the GEEK table if already exists.
    cursor.execute(f"DROP TABLE IF EXISTS {table}")
    query = """
    CREATE TABLE {table}(
        StockID INTEGER PRIMARY KEY,
        Make  varcahr(255),
        Model  varcahr(255),
        ColorID INTEGER,
        VehicleType  varcahr(255),
        CostPrice INTEGER,
        SpareParts INTEGER,
        LaborCost INTEGER,
        Registration_Date varcahr(255) ,
        Mileage INTEGER,
        PurchaseDate  varcahr(255)
    );
    """
    table  = cursor.execute(query)
    print("Table is ready")

#inserting row into the database
def insert_row():
    columns = ['StockID', 'Make', 'Model', 'ColorID', 'VehicleType', 'CostPrice','SpareParts', 'LaborCost', 'Registration_Date', 'Mileage','PurchaseDate']
    data = []
    for col in columns:
        val = input(f"Enter {col} value: ")
        try:
            int(val)
            data.append(val)
        except:
            data.append("'" + val +"'")
    val = ",".join(data)
    query = f"""
    INSERT INTO my_cars (StockID,Make,Model,ColorID,VehicleType,CostPrice,SpareParts,LaborCost,Registration_Date,Mileage,PurchaseDate)
    VALUES ({val})
    """
    output = cursor.execute(query)
    print(list(output))

#updating the database
def update_row(table= "my_cars"):
    columns = ['StockID', 'Make', 'Model', 'ColorID', 'VehicleType', 'CostPrice','SpareParts', 'LaborCost', 'Registration_Date', 'Mileage','PurchaseDate']
    
    # Collecting all IDs which exist in the table
    query = f"""
    Select StockID from {table}
    """
    output = cursor.execute(query)
    IDs = [ID for row in output for ID in row]
    
    # Exceptional handling to prevent wrong IDs
    while True:
        stockid = input("Insert StockID you want want to update: ")
        try:
            stockid = int(stockid)
        except:
            print("StockID could not be string, Try Again")
            continue
            
        if stockid in IDs:
            break
        else:
            print(f"{stockid} doesn't exist, Try Again")
            continue
    
    output = cursor.execute(f"Select * from {table} Where StockID == {stockid}")
    print(list(output))
    
    # Creating Query
    data = ""
    while True:
        col_name = input(
            f"Which column do you want to update:\n{columns}"
        )
        new_val = input(f"What should be the new value for {col_name}: ")
        
        
        try:
            new_val  = int(new_val)
            data +=  f"{col_name} = {new_val},"
        except:
            new_val  = "'" +new_val +"'"
            data += f'{col_name} = {new_val},'
            
        
        choice = input("Do you want to update another val?\n(Yes/No)\n")
        if choice.lower()[0]=="y":
            continue
        else:
            data = data.strip(",")
            break
    
    query = f"""
    UPDATE my_cars
    SET {data}
    WHERE StockID =={stockid};"""
    
    
    # Running Query
    try:
        cursor.execute(query)
        print("Update Sucessful")
    except Exception as e:
        print(e)
        print("Update Unsuccesful")
            
# update_row()

print("hello World!")