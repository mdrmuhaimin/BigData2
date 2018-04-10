from flask import Flask
from flask import render_template

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")   

@app.route('/getfile')
def getfile():
    with app.open_resource('./data/dataset_dashboard_d3.csv') as f:
        data = f.read()
    return data

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5000,debug=True)