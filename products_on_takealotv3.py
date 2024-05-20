import requests
import mysql.connector
from datetime import datetime
import time

# Define the config dictionary
config = {
    'user': 'root',
    'password': 'W7301@jqir#',
    'host': 'localhost',
    'database': 'takealot_data'
}

# Connect to the MySQL server
connection = mysql.connector.connect(**config)

# Now you can use the connection object
if connection.is_connected():
    print("Connected to MySQL database")

def insert_update_mysql(data):
    global connection  # Add this line to access the global connection variable
    # Your code here

def main():
    global connection  # Add this line to access the global connection variable

# Function to fetch data from Takealot API
def fetch_data_from_api(page):
    url = "https://seller-api.takealot.com/v2/offers"
    headers = {
        'Authorization': 'Key a3842826f6969a450a398c0c8939ff014e176da8622e741bf818f42f29dc9474c901d0acbb31596035e6ac278efb020d6b0830d368ee64badde72a8e4378efdd'
    }
    params = {
        "page_number": page,  # Changed from "page" to "page_number"
        "limit": 25
    }
    attempts = 0
    while attempts < 5:  # Allow up to 5 attempts per page in case of rate limits
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json().get('offers', [])
        elif response.status_code == 429:  # Rate limit exceeded
            print(f"Rate limit exceeded. Sleeping for 60 seconds. Attempting page {page} again.")
            time.sleep(60)
        else:
            print(f"Failed to fetch data from page {page}. Status code: {response.status_code}, Message: {response.text}")
        attempts += 1
    return []  # Return empty if all attempts fail
    
    # Handle rate limit error
    if response.status_code == 429:
        print("Rate limit exceeded. Sleeping for 60 seconds.")
        time.sleep(60)
        return fetch_data_from_api(page)  # Retry fetching data
    
    # Handle other errors
    elif response.status_code != 200:
        print(f"Failed to fetch data from page {page}. Status code: {response.status_code}, Message: {response.text}")
        return []
    
    # Successful response
    else:
        return response.json().get('offers', [])

def main():
    page = 1
    total_records = 0

    while True:
        data = fetch_data_from_api(page)
        if data:
            # Assume insert_update_mysql is defined elsewhere and handles the database update
            insert_update_mysql(data)
            total_records += len(data)
            print(f"Fetched {len(data)} records from page {page}")

            # Check if it's possibly the last page
            if len(data) < 25:
                print("Reached the last page or fewer records on the last fetched page.")
                break

            page += 1  # Move to the next page only if data is fetched successfully
        else:
            print("No more data found or failed to fetch after retries. Exiting...")
            break

    print(f"All data has been successfully added to MySQL. Total records: {total_records}")



# Function to insert or update data in MySQL
def insert_update_mysql(data):
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        query = """
        INSERT INTO products_on_takealotv3 (
            TSIN_ID, Image_URL, Offer_ID, SKU, Barcode, Product_Label_Number, 
            Selling_Price, RRP, Leadtime_Days, Leadtime_Stock, Status, Title, 
            Offer_URL, Stock_at_Takealot_CPT, Stock_at_Takealot_JHB, 
            Stock_on_Way_CPT, Stock_on_Way_JHB, Total_Stock_on_Way, 
            Stock_Cover_CPT, Stock_Cover_JHB, Total_Stock_Cover, 
            Sales_Units_CPT, Sales_Units_JHB, Stock_at_Takealot_Total, 
            Date_Created, Storage_Fee_Eligible, Discount, Discount_Shown
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            Image_URL=VALUES(Image_URL), Offer_ID=VALUES(Offer_ID), SKU=VALUES(SKU), 
            Barcode=VALUES(Barcode), Product_Label_Number=VALUES(Product_Label_Number), 
            Selling_Price=VALUES(Selling_Price), RRP=VALUES(RRP), Leadtime_Days=VALUES(Leadtime_Days), 
            Leadtime_Stock=VALUES(Leadtime_Stock), Status=VALUES(Status), Title=VALUES(Title), 
            Offer_URL=VALUES(Offer_URL), Stock_at_Takealot_CPT=VALUES(Stock_at_Takealot_CPT), 
            Stock_at_Takealot_JHB=VALUES(Stock_at_Takealot_JHB), Stock_on_Way_CPT=VALUES(Stock_on_Way_CPT), 
            Stock_on_Way_JHB=VALUES(Stock_on_Way_JHB), Total_Stock_on_Way=VALUES(Total_Stock_on_Way), 
            Stock_Cover_CPT=VALUES(Stock_Cover_CPT), Stock_Cover_JHB=VALUES(Stock_Cover_JHB), 
            Total_Stock_Cover=VALUES(Total_Stock_Cover), Sales_Units_CPT=VALUES(Sales_Units_CPT), 
            Sales_Units_JHB=VALUES(Sales_Units_JHB), Stock_at_Takealot_Total=VALUES(Stock_at_Takealot_Total), 
            Date_Created=VALUES(Date_Created), Storage_Fee_Eligible=VALUES(Storage_Fee_Eligible), 
            Discount=VALUES(Discount), Discount_Shown=VALUES(Discount_Shown);
        """
        for product in data:
            if isinstance(product, bytes):
                product = product.decode('utf-8')
                product = json.loads(product)

            leadtime_stock = product.get('leadtime_stock', [{}])
            leadtime_quantity = leadtime_stock[0].get('quantity_available', 0) if leadtime_stock else 0

            stock_at_takealot = product.get('stock_at_takealot', [{'quantity_available': 0}, {'quantity_available': 0}])
            stock_on_way = product.get('stock_on_way', [{'quantity_available': 0}, {'quantity_available': 0}])
            stock_cover = product.get('stock_cover', [{'stock_cover_days': 0}, {'stock_cover_days': 0}])
            sales_units = product.get('sales_units', [{'sales_units': 0}, {'sales_units': 0}])

            values = (
                product.get('tsin_id'),
                product.get('image_url'),
                product.get('offer_id'),
                product.get('sku'),
                product.get('barcode'),
                product.get('product_label_number'),
                product.get('selling_price'),
                product.get('rrp'),
                product.get('leadtime_days'),
                leadtime_quantity,
                product.get('status'),
                product.get('title'),
                product.get('offer_url'),
                stock_at_takealot[0]['quantity_available'],
                stock_at_takealot[1]['quantity_available'],
                stock_on_way[0]['quantity_available'],
                stock_on_way[1]['quantity_available'],
                product.get('total_stock_on_way'),
                stock_cover[0]['stock_cover_days'],
                stock_cover[1]['stock_cover_days'],
                product.get('total_stock_cover'),
                sales_units[0]['sales_units'],
                sales_units[1]['sales_units'],
                product.get('stock_at_takealot_total'),
                datetime.strptime(product.get('date_created'), '%Y-%m-%d %H:%M:%S'),
                product.get('storage_fee_eligible'),
                product.get('discount'),
                product.get('discount_shown')
            )
            cursor.execute(query, values)
        connection.commit()
    except mysql.connector.Error as e:
        print("Error in MySQL operation: ", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Main function
def main():
    page = 1
    total_records = 0
    requests_count = 0

    while True:
        data = fetch_data_from_api(page)
        if data:
            insert_update_mysql(data)
            total_records += len(data)
            print(f"Fetched {len(data)} records from page {page}")
            requests_count += 1

            # Check if 100 requests have been made
            if requests_count == 100:
                print("Reached 100 requests. Sleeping for 60 seconds.")
                time.sleep(60)  # Sleep for 60 seconds
                requests_count = 0  # Reset requests count

            page += 1  # Move to the next page only if data is fetched successfully
        else:
            print("No more data found. Exiting...")
            break

    print(f"All data has been successfully added to MySQL. Total records: {total_records}")



if __name__ == "__main__":
    main()
