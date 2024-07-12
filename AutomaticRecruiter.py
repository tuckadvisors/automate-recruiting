from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import requests
import json
from dotenv import load_dotenv
import os

class AutomaticRecruiter:
  def __init__(self):
    self.load_env()
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

  def load_env(self):
    load_dotenv()
    self.form_id = os.getenv("FORM_ID")
    self.pd_api_key = os.getenv("PD_API_KEY")
    self.pd_app_key = os.getenv("PD_APP_KEY")
    self.service_account_file_path = os.getenv("SERVICE_ACCOUNT_FILE")

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
    }

  def get_form_responses(self):
    self.responses = self.service_forms.forms().responses().list(formId=self.form_id).execute()

  def get_custom_fields(self):
    try:
      response = requests.get(url=self.pd_access_label_ids_url)
      cleaned_response = json.loads(response.text)

      person_response = requests.get(url=self.pd_access_person_label_ids_url)
      cleaned_person_response = json.loads(person_response.text)

      # This block prints out name, count pairs for each of the custom fields, so if there are any more, we can easily add them below
      # --------------------------------------------------------------------
      # count = 0
      # for i in cleaned_response["entries"]:
      #   print(i["name"], count)
      #   count += 1
      # --------------------------------------------------------------------

      self.recruiting_step = cleaned_response["entries"][6]["custom_field_label_dropdown_entries"]
      self.search_exp_graduation_year = cleaned_response["entries"][195]["custom_field_label_dropdown_entries"] # class of... dropdown
      self.term_interested_in_internship = cleaned_person_response["entries"][47]["custom_field_label_dropdown_entries"]
      self.source_referred_by = cleaned_response["entries"][66] # source / referred by

    except Exception as e:
      print("Error:", e)

  def populate_pd_val_fields(self):
    response = sorted(self.responses['responses'], key=lambda x: x['createTime'], reverse=True)[0]
    
    # Displays ordering of input fields in google form for debugging
    # --------------------------------------------------------------------
    # with open("test.json", "w") as f:
    #   json.dump(response, f)
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
          else:
            if type(data) != list:
              current_data = data["value"]

          # populate custom fields
          current_custom_field = None
          if field_num == 11: # expected graduation year
            for field in self.search_exp_graduation_year:
              if field["name"] == str(current_data):
                current_custom_field = field["id"]
            self.pd_val_fields["custom_fields"]["custom_label_3815151"] = current_custom_field
          elif field_num == 0: # term interested in internship, ft / pt
            for val in data:
              current_data = val["value"]
              for field in self.term_interested_in_internship:
                if field["name"] == str(current_data):
                  current_custom_field = field["id"]
                  self.pd_val_fields["custom_fields"]["custom_label_3815053"].append(current_custom_field)
          elif field_num == 13: # source / referred by
            self.pd_val_fields["custom_fields"]["custom_label_3844026"] = current_data + "\nApplication Date: " + datetime.today().strftime('%m-%d-%Y')

          # populate summary fields -- make sure these are sorted
          elif field_num == 4:
            self.pd_val_fields[self.pd_key_fields[field_num]] += "\nMajor: " + current_data
          elif field_num == 5:
            self.pd_val_fields[self.pd_key_fields[field_num]] += "\nOther Information: " + current_data
          elif field_num == 7:
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
    except Exception:
      pass
    
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
      except:
        pass
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
      pass

  def create_documents(self):
    try:
      for i in ["transcript", "resume"]:
        response = requests.post(
          url = self.pd_create_document_url,
          json = {
            "document": {
              "url": self.pd_val_fields[i],
              "person_id": self.new_person_id,
              "title": self.pd_link_to_name[self.pd_val_fields[i]],
              "document_type": "documents"
            }
          }
        )
        print("Added file: ", response.text)
    except Exception as e:
      print(e)

  def update_documents(self):
    try:
      index = 0
      for i in ["transcript", "resume"]:
        print("Trying to update: ", self.output_ids[index])
        response = requests.put(
          url = "https://api.pipelinecrm.com/api/v3/documents/{}?api_key={}&app_key={}".format(self.output_ids[index], self.pd_api_key, self.pd_app_key),
          json = {
            "document": {
              "url": self.pd_val_fields[i],
              "person_id": self.new_person_id,
              "title": self.pd_link_to_name[self.pd_val_fields[i]],
              "document_type": "documents"
            }
          }
        )
        print("Updated file: ", response.text)
        index += 1
    except Exception as e:
      print(e)

  def create_task(self):
    try:
      response = requests.post(
        url = "https://api.pipelinecrm.com/api/v3/calendar_entries.json?api_key={}&app_key={}".format(self.pd_api_key, self.pd_app_key),
        json = {
          "calendar_entry": {
            "name": "Review new candidate's profile",
            "category_id": "2183113",
            "association_type": "Person",
            "association_id": self.new_person_id,
            "active": True
          }
        }
      )
      print("Created task: ", response.text)
    except Exception as e:
      print("Something went wrong:", e)

  '''
  Driver method -- creates both the pd profile and the documents from the google sheet
  '''
  def main(self):
    person_exists = self.check_person_exists()
    documents_exist = self.check_documents_exist()
    # person logic
    if not person_exists:
      self.create_pd_profile()
    else:
      self.update_pd_profile()
    # document logic
    if not documents_exist:  
      self.create_documents()
    else:
      self.update_documents()
    # create task to review new profile
    # self.create_task()

if __name__ == '__main__':
  a = AutomaticRecruiter()
  a.main()
