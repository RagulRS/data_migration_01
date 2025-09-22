# Backend/vault_migration.py
import json
import requests
import pandas as pd
from datetime import datetime
from collections import OrderedDict
from pathlib import Path

def migrate_to_vault(transformed_output_file, study_name, site_number, study_country,
                     old_subj_list, new_subj_list, data_dir: Path, vault_config: dict):
    """
    transformed_output_file: path to CSV (Path or string)
    old_subj_list / new_subj_list: lists (must be same length) for subject mapping
    data_dir: Path to data folder where logs will be written
    vault_config: dict with keys VAULT_DNS, API_VERSION, USERNAME, PASSWORD
    """

    data_dir = Path(data_dir)
    transformed_output_file = Path(transformed_output_file)

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
        resp = requests.post(url, data=data)
        resp.raise_for_status()
        return resp.json().get("sessionId")

    # reusing your logic but parameterized
    def extract_failed_items(response_json, data_df=None, session_id=None):
        nonlocal failure_lines, failure_itemgs
        for item in response_json.get("items", []):
            if item.get("responseStatus") == "FAILURE":
                item_id = item.get("item_name", "N/A")
                error_msg = item.get("errorMessage", {})
                subject = item.get("subject", "N/A")
                eg_name = item.get("eventgroup_name", "N/A")
                event_name = item.get("event_name", "N/A")
                form_name = item.get("form_name", "N/A")
                item_group = item.get("itemgroup_name", "N/A")
                value = item.get("value", "N/A")
                line = (f"ITEM FAILURE - SUBJECT: {subject}, EVENT NAME: {event_name}, FORM NAME: {form_name}, ITEM NAME: {item_id}, VALUE: {value}, ERROR: {error_msg}")
                if "Unique item group cannot be found" in str(error_msg):
                    failure_itemgs.append(line)
                    # For simplicity: we won't trigger item group creation here automatically
                else:
                    failure_lines.append(line)
        for event in response_json.get("events", []):
            if event.get("responseStatus") == "FAILURE":
                failure_lines.append(f"EVENT DATE FAILURE - SUBJECT: {event.get('subject','N/A')}, EVENT: {event.get('event_name','N/A')}, ERROR: {event.get('errorMessage',{})}")

    # simplified set_form_items and other helpers - kept shorter and robust
    def set_form_items(session_id, subj_id, event_group, event_name, data_df, form_names=[]):
        url = f"https://{VAULT_DNS}/api/{API_VERSION}/app/cdm/items"
        forms_payload = []
        Tot_forms = []
        if form_names:
            for form_name in form_names:
                filtered_df = data_df[(data_df['Form Name'] == form_name) & (data_df['Event Name'] == event_name)]
                items = []
                for _, row in filtered_df.iterrows():
                    item_name = row.get('Item Name', "")
                    value = row.get('Item Data', "")
                    item_group = row.get('Item Group', "")
                    if pd.notna(value) and str(value).strip() != "" and pd.notna(item_name) and str(item_name).strip() != "":
                        if isinstance(value, float) and value.is_integer():
                            value = str(int(value))
                        elif isinstance(value, (int, float)):
                            value = str(value)
                        elif isinstance(value, datetime):
                            value = value.strftime("%Y-%m-%d")
                        else:
                            value = str(value).strip()
                        items.append({"itemgroup_name": item_group, "item_name": item_name, "value": value})
                if items:
                    Tot_forms.append(form_name)
                    forms_payload.append({
                        "study_country": study_country, "subject": subj_id, "site": site_number,
                        "eventgroup_name": event_group, "event_name": event_name, "form_name": form_name, "items": items
                    })

        if forms_payload:
            payload = {"study_name": study_name, "forms": forms_payload}
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': session_id}
            resp = requests.post(f"https://{VAULT_DNS}/api/{API_VERSION}/app/cdm/items", headers=headers, json=payload)
            try:
                resp_json = resp.json()
            except Exception:
                resp_json = {}
            extract_failed_items(resp_json, data_df, session_id)
            # attempt submit
            try:
                forms_to_submit = [{"study_country": study_country, "subject": subj_id, "site": site_number,
                                    "eventgroup_name": event_group, "event_name": event_name, "form_name": fn} for fn in Tot_forms]
                sub_payload = {"study_name": study_name, "forms": forms_to_submit}
                headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': session_id}
                resp2 = requests.post(f"https://{VAULT_DNS}/api/{API_VERSION}/app/cdm/forms/actions/submit", headers=headers, json=sub_payload)
                try:
                    extract_failed_items(resp2.json())
                except Exception:
                    pass
            except Exception:
                pass

    def get_forms(session_id, subj_id, event_group, event_name):
        url = f"https://{VAULT_DNS}/api/{API_VERSION}/app/cdm/forms?study_name={study_name}&study_country={study_country}&site={site_number}&subject={subj_id}&eventgroup_name={event_group}&event_name={event_name}"
        headers = {'Authorization': session_id, 'Content-Type': 'application/json'}
        resp = requests.get(url, headers=headers)
        try:
            data = resp.json()
            return [f.get("form_name") for f in data.get("forms", [])]
        except Exception:
            return []

    def process_event(session_id, subj_id, event_group, event_name, data_df):
        # fetch list of forms and then post items
        forms = get_forms(session_id, subj_id, event_group, event_name)
        if forms:
            set_form_items(session_id, subj_id, event_group, event_name, data_df, form_names=forms)

    # --- main execution
    migration_log = []
    try:
        session_id = authenticate()
        target_data = pd.read_csv(transformed_output_file)
        # if old/new subj lists given map them else skip
        if not old_subj_list:
            return {"skipped": True, "message": "No subject mapping provided. Provide subjects mapping in the frontend."}
        for i, new_subj in enumerate(new_subj_list):
            old_subj = old_subj_list[i] if i < len(old_subj_list) else old_subj_list[0]
            for eg in target_data['Event Group Name'].dropna().unique().tolist():
                data_df = target_data[(target_data['Event Group Name'] == eg) & (target_data['Subject'] == old_subj) & (target_data['Item Data'].notna()) & (target_data['Item Data'].astype(str).str.strip() != "")]
                data_df = data_df.drop_duplicates(subset=['Event Name','Form Name','Item Name','Subject']) if not data_df.empty else data_df
                # for each event name in this group, attempt to set event date and then forms
                for event_name in data_df['Event Name'].dropna().unique().tolist():
                    # set event date if found (mode)
                    date_series = data_df[data_df['Event Name']==event_name]['Event Date'].dropna()
                    if not date_series.empty:
                        most_common = pd.to_datetime(date_series.mode().iloc[0], dayfirst=True, errors='coerce')
                        if pd.notna(most_common):
                            date_str = most_common.strftime('%Y-%m-%d')
                            # set event date API - simplified endpoint
                            url = f"https://{VAULT_DNS}/api/{API_VERSION}/app/cdm/events/actions/setdate"
                            payload = {"study_name": study_name, "events": [{
                                "study_country": study_country, "site": site_number, "subject": new_subj,
                                "eventgroup_name": eg, "event_name": event_name, "date": date_str
                            }]}
                            headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': session_id}
                            resp = requests.post(url, headers=headers, json=payload)
                            try:
                                extract_failed_items(resp.json(), data_df, session_id)
                            except Exception:
                                pass
                    # set form items & submit
                    process_event(session_id, new_subj, eg, event_name, data_df)

        # write failure logs
        with open(FAILED_ITEMS_OUTPUT_FILE, "w") as f:
            for line in failure_lines:
                f.write(line + "\n")
        with open(OUTPUT_LOG_FILE, "w") as f:
            for line in failure_itemgs:
                f.write(line + "\n")

        return {"skipped": False, "failures": failure_lines, "itemgroups_failures": failure_itemgs}
    except requests.exceptions.RequestException as e:
        return {"error": f"API request failed: {str(e)}"}
    except Exception as e:
        return {"error": str(e)}
