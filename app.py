from flask import Flask, jsonify
from flask_cors import CORS
from flask_cors import cross_origin
from AutomaticRecruiter import AutomaticRecruiter

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, allow_headers="Content-Type", supports_credentials=True) 

@app.route("/", methods=["GET"])
@cross_origin()
def index():
  return "api home"

@app.route("/updatePD", methods=["POST"])
@cross_origin()
def update_pd():
  try:
    a = AutomaticRecruiter()
    a.main()
    return jsonify({"response": "successfully updated pipeline"}), 200
  except Exception as e:
    print(e)
    return jsonify({"response": "unable to update pipeline"}), 400
  
if __name__ == '__main__':
    app.run(port=8080)