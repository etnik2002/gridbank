from flask import Flask
from routes.product import fetch_products_from_apis

app = Flask(__name__)

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route("/products")
def fetch_products():
    products = fetch_products_from_apis()
    return products

if __name__ == '__main__':  
   app.run()  