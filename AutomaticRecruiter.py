from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import requests
import json
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
import os

class AutomaticRecruiter:
  def __init__(self):
    load_dotenv()
    self.get_secret()
    self.setup_gcp()
    self.set_urls()
    self.init_maps()
    self.get_form_responses()
    self.get_custom_fields()

  def setup_gcp(self):
    self.SCOPES = ['https://www.googleapis.com/auth/forms', 'https://www.googleapis.com/auth/drive']
    self.SERVICE_ACCOUNT_FILE = self.service_account_file_path
    self.credentials = service_account.Credentials.from_service_account_file(
      self.SERVICE_ACCOUNT_FILE, scopes=self.SCOPES)
    self.service_forms = build('forms', 'v1', credentials=self.credentials)
    self.service_drive = build('drive', 'v3', credentials=self.credentials)
  
  def get_secret(self):
    # get secret, region name from .env file
    secret_name = os.getenv("SECRET_NAME")
    region_name = os.getenv("REGION_NAME")
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])
    except ClientError as e:
        raise e
    
    # set each of the 4 secrets
    self.form_id = get_secret_value_response['recruiting_form_id']
    self.pd_api_key = get_secret_value_response['recruiting_pd_api_key']
    self.pd_app_key = get_secret_value_response['recruiting_pd_app_key']
    self.service_account_file_path = get_secret_value_response['recruiting_service_account_file']

  def set_urls(self):
    self.pd_access_label_ids_url = "https://api.pipelinecrm.com/api/v3/admin/custom_field_labels.json?api_key={}&app_key={}".format(self.pd_api_key, self.pd_app_key)
    self.pd_access_person_label_ids_url = "https://api.pipelinecrm.com/api/v3/admin/person_custom_field_labels.json?api_key={}&app_key={}".format(self.pd_api_key, self.pd_app_key)
    self.pd_create_person_url = "https://api.pipelinecrm.com/api/v3/people?api_key={}&app_key={}".format(self.pd_api_key, self.pd_app_key)
    self.pd_create_document_url = "https://api.pipelinecrm.com/api/v3/documents?api_key={}&app_key={}".format(self.pd_api_key, self.pd_app_key)

  def init_maps(self):
    # Map to key into response fields
    self.pd_key_fields = {
      0: "Term of Employment",
      1: "linked_in_url",
      2: "transcript",
      3: "company_name",
      4: "summary",
      5: "summary",
      6: "last_name",
      7: "summary",
      8: "summary",
      9: "first_name",
      10: "mobile",
      11: "Expected Graduation Year",
      12: "summary",
      13: "Source / Referred By",
      14: "email",
      15: "resume",
      16: "Term Interested in Internship",
    }

    # Map to fill values of response fields
    self.pd_val_fields = {
      "linked_in_url": "",
      "transcript": "",
      "company_name": "",
      "summary": "",
      "last_name": "",
      "first_name": "",
      "custom_fields": {
        "custom_label_3815151": "", # Class of
        "custom_label_3843630": "", # recruiting step
        "custom_label_3815053": [], # Term interested in internship
        "custom_label_3844026": ""  # source / referred by
      },
      "email": "",
      "resume": "",
      "mobile": "",
      "position": "Student"
    }

  def get_form_responses(self):
    self.responses = self.service_forms.forms().responses().list(formId=self.form_id).execute()

  def get_custom_fields(self):
    try:
      response = requests.get(url=self.pd_access_label_ids_url)
      cleaned_response = json.loads(response.text)

      person_response = requests.get(url=self.pd_access_person_label_ids_url)
      cleaned_person_response = json.loads(person_response.text)

      # use a loop to calculate indeces in case fields are added to PD and the ordering changes
      self.recruiting_step_index = 0
      self.search_exp_graduation_year_index = 0
      self.term_interested_in_internship_index = 0
      self.source_referred_by_index = 0

      # get indeces for custom fields from the first response
      search_idx = 0
      for i in cleaned_response["entries"]:
        if i["name"] == "Recruiting Steps":
          self.recruiting_step_index = search_idx
        elif i["name"] == "Source / Referred By":
          self.source_referred_by_index = search_idx
        search_idx += 1

      # do the same for the person response
      search_idx = 0
      for i in cleaned_person_response["entries"]:
        if i["name"] == "Term Interested in Internship":
          self.term_interested_in_internship_index = search_idx
        elif i["name"] == "Class of ...":
          self.search_exp_graduation_year_index = search_idx
        search_idx += 1

      # This block prints out name, count pairs for each of the custom fields, so if there are any more, we can easily add them below
      # --------------------------------------------------------------------
      # count = 0
      # for i in cleaned_response["entries"]:
      #   print(i["name"], count)
      #   count += 1

      # print("\n--------------\n")

      # count = 0
      # for i in cleaned_person_response["entries"]:
      #   print(i["name"], count)
      #   count += 1
      # --------------------------------------------------------------------

      # print(self.recruiting_step_index)
      # print(self.search_exp_graduation_year_index)
      # print(self.term_interested_in_internship_index)

      self.recruiting_step = cleaned_response["entries"][self.recruiting_step_index]["custom_field_label_dropdown_entries"]
      self.search_exp_graduation_year = cleaned_person_response["entries"][self.search_exp_graduation_year_index]["custom_field_label_dropdown_entries"] # class of... dropdown
      self.term_interested_in_internship = cleaned_person_response["entries"][self.term_interested_in_internship_index]["custom_field_label_dropdown_entries"]
      self.source_referred_by = cleaned_response["entries"][self.source_referred_by_index] # source / referred by

    except Exception as e:
      print("Error:", e)

  def populate_pd_val_fields(self):
    try:
      response = sorted(self.responses['responses'], key=lambda x: x['createTime'], reverse=True)[0]
      
      # Displays ordering of input fields in google form for debugging
      # --------------------------------------------------------------------
      with open("test.json", "w") as f:
        json.dump(response, f)
      # --------------------------------------------------------------------
      
      field_num = 0
      self.pd_link_to_name = {} # Mapping links to titles for the documents
      outer_answers = response["answers"]
      for key in outer_answers:
        # preserve order
        for inner_answer in sorted(outer_answers[key]):
          if "Answers" in inner_answer:
            inner_inner_answer = outer_answers[key][inner_answer]
            if len(inner_inner_answer["answers"]) > 1:
              data = inner_inner_answer["answers"]
            else:
              data = inner_inner_answer["answers"][0]
            
            current_data = None
            # handle files
            if type(data) != list and "fileId" in data.keys():
              file_id = data["fileId"]
              file_metadata = self.service_drive.files().get(fileId=file_id, fields='name,webContentLink').execute()
              current_data = file_metadata["webContentLink"]
              # map the downloadable link to the title
              self.pd_link_to_name[current_data] = file_metadata["name"]
            else: # not a file
              if type(data) != list:
                current_data = data["value"]

            # populate custom fields
            # print(current_data)
            current_custom_field = None
            if field_num == 11: # expected graduation year
              for field in self.search_exp_graduation_year:
                if field["name"] == str(current_data):
                  current_custom_field = field["id"]
              self.pd_val_fields["custom_fields"]["custom_label_3815151"] = current_custom_field
            
            # problematic field
            elif field_num == 0: # term interested in internship, ft / pt
              # standardize the data format if multiple terms are selected
              idx = 0
              if type(data) == list:
                nd = {}
                for d in data:
                  for _, v in d.items():
                    nd[idx] = v
                    idx += 1
                data = nd
              for val in data.values():
                current_data = val
                for field in self.term_interested_in_internship:
                  if field["name"] == str(current_data):
                    current_custom_field = field["id"]
                    self.pd_val_fields["custom_fields"]["custom_label_3815053"].append(current_custom_field)
            elif field_num == 13: # source / referred by
              self.pd_val_fields["custom_fields"]["custom_label_3844026"] = current_data + "\nApplication Date: " + datetime.today().strftime('%m-%d-%Y')

            # populate summary fields -- make sure these are sorted
            elif field_num == 5:
              self.pd_val_fields[self.pd_key_fields[field_num]] += "\nMajor: " + current_data
            elif field_num == 4:
              self.pd_val_fields[self.pd_key_fields[field_num]] += "\nOther Information: " + current_data
            elif field_num == 7:
              # has to be treated the same way as the term of employment data
              if type(data) == list:
                current_data = ""
                idx = 0
                for d in data:
                  for _, v in d.items():
                    if idx < len(data) - 1:
                      current_data += (v + ", ")
                    else:
                      current_data += v
                    idx += 1
              else:
                current_data = data["value"]
              self.pd_val_fields[self.pd_key_fields[field_num]] += "\nHours: " + current_data
            elif field_num == 8:
              self.pd_val_fields[self.pd_key_fields[field_num]] += "\nGPA: " + current_data
            elif field_num == 12:
              self.pd_val_fields[self.pd_key_fields[field_num]] += "\nApplied before: " + current_data
            else:
              self.pd_val_fields[self.pd_key_fields[field_num]] = current_data

            field_num += 1
      
      # set the recruiting step
      self.pd_val_fields["custom_fields"]["custom_label_3843630"] = "6960371"
    
    except Exception as e:
      print("Error populating dict: ", e)

  def check_person_exists(self):
    self.id_to_update = None
    self.populate_pd_val_fields()
    self.pd_check_person_url = "https://api.pipelinecrm.com/api/v3/people?conditions[person_linked_in_url]={}&conditions[person_first_name]={}&conditions[person_last_name]={}&api_key={}&app_key={}".format(
      self.pd_val_fields["linked_in_url"], self.pd_val_fields["first_name"], self.pd_val_fields["last_name"], self.pd_api_key, self.pd_app_key
    )
    response = requests.get(
      url = self.pd_check_person_url
    )
    try:
      self.id_to_update = json.loads(response.text)["entries"][0]["id"]
      return self.id_to_update
    except Exception as e:
      print("Error while checking person exists: ", e)
    
  def check_documents_exist(self):
    self.output_ids = []
    for i in ["transcript", "resume"]:
      self.pd_check_document_url = "https://api.pipelinecrm.com/api/v3/documents?conditions[document_name]={}&api_key={}&app_key={}".format(
        self.pd_link_to_name[self.pd_val_fields[i]], self.pd_api_key, self.pd_app_key
      )
      response = requests.get(
        url = self.pd_check_document_url
      )
      try:
        self.output_ids.append(json.loads(response.text)["entries"][0]["id"])
      except Exception as e:
        print("Document doesn't exist: ", e)
    return self.output_ids

  def create_pd_profile(self):
    print("Trying to create profile")
    print(self.pd_val_fields)
    response = requests.post(
      url = self.pd_create_person_url,
      json = {"person": self.pd_val_fields}
    )
    if response.status_code == 200:
      self.new_person_id = json.loads(response.text)["id"]
      print("Succesfully created profile: ", response.text)
    else:
      print(response.text)

  def update_pd_profile(self):
    print("Trying to update: ", self.id_to_update)
    # check this
    response = requests.put(
      url = "https://api.pipelinecrm.com/api/v3/people/{}?api_key={}&app_key={}".format(self.id_to_update, self.pd_api_key, self.pd_app_key),
      json = {"person": self.pd_val_fields}
    )
    if response.status_code == 200:
      self.new_person_id = json.loads(response.text)["id"]
      print("Succesfully updated profile: ", response.text)
    else:
      print("Something went wrong: ", response.text)

  def create_documents(self):
    document_fields = ["transcript", "resume"]
    index = 0
    try:
      for _ in range(0, 2 - self.checked):
        response = requests.post(
          url = self.pd_create_document_url,
          json = {
            "document": {
              "url": self.pd_val_fields[document_fields[index]],
              "person_id": self.new_person_id,
              "title": self.pd_link_to_name[self.pd_val_fields[document_fields[index]]],
              "document_type": "documents"
            }
          }
        )
        print("Added file: ", response.text)
        index += 1
    except Exception as e:
      print(e)

  def update_documents(self):
    document_fields = ["transcript", "resume"]
    self.checked = 0
    index = 0
    try:
      for i in self.documents_exist:
        print("Trying to update: ", i)
        response = requests.put(
          url = "https://api.pipelinecrm.com/api/v3/documents/{}?api_key={}&app_key={}".format(i, self.pd_api_key, self.pd_app_key),
          json = {
            "document": {
              "url": self.pd_val_fields[document_fields[index]],
              "person_id": self.new_person_id,
              "title": self.pd_link_to_name[self.pd_val_fields[document_fields[index]]],
              "document_type": "documents"
            }
          }
        )
        print("Updated file: ", response.text)
        self.checked += 1
        index += 1
    except Exception as e:
      print("Error updating document: ", e)

  '''
  Driver method -- creates both the pd profile and the documents from the google sheet
  '''
  def main(self):
    self.person_exists = self.check_person_exists()
    self.documents_exist = self.check_documents_exist()
    print(self.documents_exist)
    # person logic
    if not self.person_exists:
      self.create_pd_profile()
    else:
      self.update_pd_profile()
    
    # document logic
    self.update_documents()
    # create the documents that don't exist    
    self.create_documents()
    
if __name__ == '__main__':
  a = AutomaticRecruiter()
  a.main()
