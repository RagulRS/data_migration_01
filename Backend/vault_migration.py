# Backend/vault_migration.py
import json
import requests
import pandas as pd
from datetime import datetime
from collections import OrderedDict
from pathlib import Path

def migrate_to_vault(transformed_output_file, STUDY_NAME, SITE_NUMBER, STUDY_COUNTRY,
                     old_subj_list, new_subj_list, data_dir: Path, vault_config: dict,target_spec):
    """
    transformed_output_file: path to CSV (Path or string)
    old_subj_list / new_subj_list: lists (must be same length) for subject mapping
    data_dir: Path to data folder where logs will be written
    vault_config: dict with keys VAULT_DNS, API_VERSION, USERNAME, PASSWORD
    """

    data_dir = Path(data_dir)
    TRANSFORMED_OUTPUT_FILE = Path(transformed_output_file)
    TARGET_SPEC_FILE=Path(target_spec)

    FAILED_ITEMS_OUTPUT_FILE = data_dir / "failed_items_output.txt"
    OUTPUT_LOG_FILE = data_dir / "output_log.txt"

    # if config incomplete -> skip
    if not (vault_config.get("VAULT_DNS") and vault_config.get("USERNAME") and vault_config.get("PASSWORD")):
        return {"skipped": True, "message": "Vault credentials not provided."}

    VAULT_DNS = vault_config["VAULT_DNS"]
    API_VERSION = vault_config.get("API_VERSION", "v23.2")
    USERNAME = vault_config["USERNAME"]
    PASSWORD = vault_config["PASSWORD"]

    failure_lines = []
    failure_itemgs = []

    def authenticate():
        url = f"https://{VAULT_DNS}/api/{API_VERSION}/auth"
        data = {"username": USERNAME, "password": PASSWORD}
        response = requests.post(url, data=data)
        response.raise_for_status()
        print("Authentication successful.")
        return response.json()["sessionId"]

    def get_event(eg):
        temp_spec=design_spec[design_spec['Event Group Name']==eg]
        event_names=temp_spec['Event Name'].dropna().unique().tolist()
        return event_names
    
    def get_trigger_form_list(event_group,event_name):
        temp_spec=trigger_form_spec[(trigger_form_spec['Event Group Name']==event_group)&(trigger_form_spec['Event Name']==event_name)]
        form_names=temp_spec['Form Name'].dropna().unique().tolist()
        return form_names
        
    def get_trigger_ig_list():
        temp_spec= form_def_spec[form_def_spec['IG Rep']=='Yes']
        ig_list=temp_spec['Item Group Name'].dropna().unique().tolist()
        return ig_list

    def set_event_date(session_id, subj_id, event_group, event_name, date):
        url = f"https://{VAULT_DNS}/api/{API_VERSION}/app/cdm/events/actions/setdate"
        payload = {
            "study_name": STUDY_NAME,
            "events": [{
                "study_country": STUDY_COUNTRY, "site": SITE_NUMBER, "subject": subj_id,
                "eventgroup_name": event_group, "event_name": event_name, "date": date
            }]
        }
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': session_id}
        response = requests.post(url, headers=headers, json=payload)
        #print("Set Event Date Response:", response.json())
        extract_failed_items(response.json())

    def get_forms(session_id, subj_id, event_group, event_name):
        url = f"https://{VAULT_DNS}/api/{API_VERSION}/app/cdm/forms?study_name={STUDY_NAME}&study_country={STUDY_COUNTRY}&site={SITE_NUMBER}&subject={subj_id}&eventgroup_name={event_group}&event_name={event_name}"
        headers = {'Authorization': session_id, 'Content-Type': 'application/json'}
        response = requests.get(url, headers=headers)
        response_data = response.json()
        return list(set([form.get("form_name") for form in response_data.get("forms", [])]))
        
    def set_form_items(session_id, subj_id, event_group, event_name, data_df,form_names=[],item_group=None,form=None):
        url = f"https://{VAULT_DNS}/api/{API_VERSION}/app/cdm/items"
        #print("Inside Set form items")
        forms_payload = []
        data_forms = []
        if form_names and item_group is None:
            for form_name in form_names:
                #print("Inside if part")
                items = []
                filtered_df = data_df[(data_df['Form Name'] == form_name) & (data_df['Event Name'] == event_name)]
                if not filtered_df.empty:
                    for _, row in filtered_df.iterrows():
                        item_name = row.get('Item Name',"")
                        value = row.get('Item Data',"")
                        item_group = row.get('Item Group',"")
                        if pd.notna(value) and value != "" and value != " " and pd.notna(item_name) and item_name != "" and item_name != " " and pd.notna(item_group) and item_group != " ":
                            # if item_group in rep_ig_list:
                            #     trigger_itemgs(session_id, subj_id, event_group, event_name, form_name,item_group)
                            if isinstance(value, float) and value.is_integer():
                                value = str(int(value))
                            elif isinstance(value, (int, float)):
                                value = str(value)
                            elif isinstance(value, datetime):
                                value = value.strftime("%Y-%m-%d")  # or your preferred format
                            else:
                                value = str(value).strip()

                            items.append({
                                "itemgroup_name": item_group,
                                "item_name": item_name,
                                "value": value
                            })
                            if form_name not in data_forms:
                                data_forms.append(form_name)
                    if items:
                        forms_payload.append({
                            "study_country": STUDY_COUNTRY, "subject": subj_id, "site": SITE_NUMBER,
                            "eventgroup_name": event_group, "event_name": event_name, "form_name": form_name, "items": items
                        })
        else:
            items = []
            filtered_df = data_df[(data_df['Form Name'] == form) & (data_df['Event Name'] == event_name) & (data_df['Item Group'] == item_group)]
            #print("Inside else part")
            if not filtered_df.empty:
                for _, row in filtered_df.iterrows():
                    item_name = row.get('Item Name')
                    value = row.get('Item Data')
                    item_group = row.get('Item Group')
                    if pd.notna(value) and str(value).strip() != "" and pd.notna(item_name) and str(item_name).strip() != "":
                        items.append({"itemgroup_name": item_group, "item_name": item_name, "value": str(value)})
                        if form not in data_forms:
                            data_forms.append(form)
                if items:
                    forms_payload.append({
                        "study_country": STUDY_COUNTRY, "subject": subj_id, "site": SITE_NUMBER,
                        "eventgroup_name": event_group, "event_name": event_name, "form_name": form, "items": items
                    })

        if forms_payload:
            payload = json.dumps({"study_name": STUDY_NAME, "forms": forms_payload}, indent=2)
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': session_id}
            response = requests.post(url, headers=headers, data=payload)
            response_json = response.json()
            extract_failed_items(response_json,data_df,session_id)
            submit_form(session_id, subj_id, event_group, event_name, data_forms)

    def submit_form(session_id, subj_id, event_group, event_name, form_names):
        url = f"https://{VAULT_DNS}/api/{API_VERSION}/app/cdm/forms/actions/submit"
        forms_to_submit = [{"study_country": STUDY_COUNTRY, "subject": subj_id, "site": SITE_NUMBER,
                            "eventgroup_name": event_group, "event_name": event_name, "form_name": fn} for fn in form_names]
        payload = json.dumps({"study_name": STUDY_NAME, "forms": forms_to_submit})
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': session_id}
        response = requests.post(url, headers=headers, data=payload)
        response_json = response.json()
        #print("Submit Form Response:", response_json)
        extract_failed_items(response_json)

    def extract_failed_items(response_json,data_df=None,session_id=None):
        global failure_lines, failure_itemgs
        itemgs_list=[]
        for item in response_json.get("items", []):
            if item.get("responseStatus") == "FAILURE":
                item_id = item.get("item_name", "N/A")
                error_msg = item.get("errorMessage", {})
                subject = item.get("subject", "N/A")
                eg_name=item.get("eventgroup_name", "N/A")
                event_name = item.get("event_name", "N/A")
                form_name = item.get("form_name", "N/A")
                item_group = item.get("itemgroup_name", "N/A")
                value = item.get("value", "N/A")
                line = (f"ITEM FAILURE - SUBJECT: {subject}, EVENT NAME: {event_name}, FORM NAME: {form_name}, ITEM NAME: {item_id}, VALUE: {value}, ERROR: {error_msg}")
                if "Unique item group cannot be found" in error_msg:
                     failure_itemgs.append(line)
                     if item_group not in itemgs_list:
                        itemgs_list.append(item_group)
                        trigger_itemgs(session_id, subject, eg_name, event_name, form_name, item_group)
                        set_form_items(session_id, subject, eg_name, event_name, data_df,form=form_name,item_group=item_group)
                else:
                    failure_lines.append(line)
        for event in response_json.get("events", []):
            if event.get("responseStatus") == "FAILURE":
                failure_lines.append(f"EVENT DATE FAILURE - SUBJECT: {event.get('subject', 'N/A')}, "
                                     f"EVENT: {event.get('event_name', 'N/A')}, DATE: {event.get('date', 'N/A')}, "
                                     f"ERROR: {event.get('errorMessage', {})}")
    

    def trigger_itemgs(session_id, subj_id, event_group, event_name, form_name,item_group):
        url = f"https://{VAULT_DNS}/api/{API_VERSION}/app/cdm/itemgroups"
        payload = json.dumps({
        "study_name": STUDY_NAME,
        "itemgroups": [
            {
            "study_country": STUDY_COUNTRY,
            "site": SITE_NUMBER,
            "subject": subj_id,
            "eventgroup_name": event_group,
            "event_name": event_name,
            "form_name": form_name,
            "itemgroup_name": item_group
            }
        ]
        })
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': session_id}
        response = requests.post(url, headers=headers, data=payload)
        #response_json = response.json()
        #print("trigger itemgs Response:", response_json)

    def trigger_forms(session_id, event_group, subj_id,event_name,form_names):
        url = f"https://{VAULT_DNS}/api/{API_VERSION}/app/cdm/forms"    
        forms = [
        {
            "study_country": STUDY_COUNTRY,
            "site": SITE_NUMBER,
            "subject": subj_id,
            "eventgroup_name": event_group,
            "event_name": event_name,
            "form_name": form_name,
        }
        for form_name in form_names
        ]

        # Final payload
        payload = json.dumps({
        "study_name": STUDY_NAME,
        "forms": forms
        })

        headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': session_id}
        response = requests.post(url, headers=headers, data=payload)
        response_json = response.json()
        print("trigger forms Response:", response_json)


    def get_event_date(event_group, event_name, data_df):
        filtered_df = data_df[(data_df['Event Group Name'] == event_group) & (data_df['Event Name'] == event_name)]
        if filtered_df.empty or 'Event Date' not in filtered_df.columns:
            return None
        most_common_date = filtered_df['Event Date'].mode()
        most_common_date = pd.to_datetime(most_common_date, dayfirst=True)
        return most_common_date.iloc[0].strftime('%Y-%m-%d') if not most_common_date.empty else None

        
    def process_event(session_id, subj_id, event_group, event_name, data_df):
        old_forms = []
        trigger_form_list=get_trigger_form_list(event_group,event_name)
        if trigger_form_list:
            trigger_forms(session_id, event_group, subj_id,event_name,trigger_form_list)
            #print("triggered forms",trigger_form_list)
        while True:
            new_forms = get_forms(session_id, subj_id, event_group, event_name)
            if set(old_forms) == set(new_forms):
                print(f"Forms for event '{event_name}' are stable.")
                break
            else:
                print(f"New forms detected for event '{event_name}'. Processing...")
                #print("new forms before",new_forms)
                for form in trigger_form_list:
                    if form not in new_forms:
                        new_forms.append(form)
                #print("new forms after",new_forms)
                form_list = list(set(new_forms) - set(old_forms))
                if form_list:
                    set_form_items(session_id, subj_id, event_group, event_name,data_df, form_list)
                old_forms = new_forms

    def process_events_until_stable(session_id, subj_id, event_group, data_df):
        
        new_events = get_event(event_group)
        print("events",new_events)
        if new_events:
            print("New events detected. Processing...")
            #new_events = sorted(new_events, key=lambda x: unique_event_order.index(x) if x in unique_event_order else float('inf'))
            for event_name in new_events:                
                date = get_event_date(event_group, event_name, data_df)
                print("execution event",event_name)
                if date:
                    set_event_date(session_id, subj_id, event_group, event_name, date)
                    process_event(session_id, subj_id, event_group, event_name, data_df)

            
    # --- Main execution for data migration ---
    try:
        session_id = authenticate()
        
        design_spec = pd.read_excel(TARGET_SPEC_FILE, sheet_name="Schedule - Tree")
        form_def_spec=pd.read_excel(TARGET_SPEC_FILE, sheet_name="Form Definitions") 
        trigger_form_spec=design_spec[design_spec['Repeats']=='Yes']       
        event_groups = design_spec['Event Group Name'].dropna().unique().tolist()
        rep_ig_list =get_trigger_ig_list()
        #event_groups=['eg_SCR']
        #event_names = design_spec['Event Name'].dropna().unique().tolist()
        
        
        target_data = pd.read_csv(TRANSFORMED_OUTPUT_FILE)
        null_values =[ '', ' ', 'NAN', 'nan', None]
        
        # if old/new subj lists given map them else skip
        if not old_subj_list:
            return {"skipped": True, "message": "No subject mapping provided. Provide subjects mapping in the frontend."}
        for i, new_subj in enumerate(new_subj_list):
            old_subj = old_subj_list[i] if i < len(old_subj_list) else old_subj_list[0]
            for eg in event_groups:
                data_df = target_data[(target_data['Event Group Name'] == eg) & (target_data['Subject'] == old_subj) & (target_data['Item Data'].notna()) & (~target_data['Item Data'].isin(null_values))]
                data_df = data_df.drop_duplicates(subset=['Event Name','Form Name','Item Name','Subject']) if not data_df.empty else data_df
                process_events_until_stable(session_id, new_subj, eg, data_df)
                    
    except requests.exceptions.RequestException as e:
        print(f"An API error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Write failure logs
        with open(FAILED_ITEMS_OUTPUT_FILE, "w") as f:
            for line in failure_lines:
                f.write(line + "\n")
        with open(OUTPUT_LOG_FILE, "w") as f:
            for line in failure_itemgs:
                f.write(line + "\n")
        print(f"\nData migration process finished. Check '{FAILED_ITEMS_OUTPUT_FILE}' and '{OUTPUT_LOG_FILE}' for any errors.")

