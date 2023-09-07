from flask import Flask, jsonify, request

app = Flask(__name__)
@app.route('/')
def root():
    return "Hello globant"

@app.route("/employee/<employee_id>")
def get_employee(employee_id):
    user = {'id':employee_id,'name':'Dario','date':'2023-09-27T06:21:37Z'}
    return jsonify(user), 200


@app.route("/hired_employees", methods=['POST'])
def create_new_em():
    data = request.get_json()
    data['status']= "user created"
    return jsonify(data), 201

if __name__ == '__main__':
    app.run(debug=True)

