from flask import Flask, request, jsonify


app = Flask(__name__)


@app.route('/get_task', methods=['GET'])
def get_task():
    name = request.args.get('name')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)