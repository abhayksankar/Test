#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os 
import re
import psycopg2
import time
import shutil
import tensorflow as tf
import os
import pathlib
from tensorflow.keras.models import load_model
from tensorflow.keras.applications.xception import preprocess_input
import numpy as np
from tensorflow.keras.preprocessing import image
from PIL import Image as image
import piexif
from datetime import datetime, timedelta
import json
import imageio.v2 as imageio
import pytz
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import traceback
from PIL.ExifTags import TAGS
import gspread
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
import logging
import subprocess
import sys


# In[2]:


model1 = load_model('filtered(5K).h5')


# In[3]:


model2 = load_model('filtered_latest(5K).h5')


# In[4]:


model3 = load_model('filtered_latest1(5K).h5')


# In[5]:


db_params = {
    'host': 'localhost',
    'port': '5432',
    'database': 'propvision_test',
    'user': 'postgres',
    'password': 'ecesis'
}

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()


recieved_json = 'C:\\Users\\USER\\Downloads\\Test_Folder1\\received_test.json'
credentials_file = 'C:\\Users\\USER\\Downloads\\Jupyter\\PropVision_v2\\Test\\credentials.json'

gc = gspread.service_account(filename=credentials_file)

spreadsheet  = gc.open_by_url('https://docs.google.com/spreadsheets/d/1amdgbpdLOs-vwJetkMH8eL2ej2E72_noZgCZqT8XyLA/edit?usp=sharing')

# In[12]:

logging.basicConfig(
    filename='C:\\Users\\USER\\Downloads\\Jupyter\\PropVision Classifier\\test.log',  
    level=logging.INFO,          
    format='%(asctime)s - %(levelname)s - %(message)s',  
    datefmt='%Y-%m-%d %H:%M:%S'
)
# In[13]:

truncated_file = "C:\\Users\\USER\\Downloads\\Test_Folder1\\truncated_orders.txt"

# In[16]:


def create_raw_folder(parent_folder):
    new_folder_path = os.path.join(parent_folder, 'RAW')
    if not os.path.exists(new_folder_path):
        os.makedirs(new_folder_path)
        
def create_model_folder(parent_folder):
    new_folder_path = os.path.join(parent_folder, 'model')
    contents = os.listdir(parent_folder)
    if 'Completed' not in contents and 'Verified' not in contents and 'Rejected' not in contents and not os.path.exists(new_folder_path):
        os.makedirs(new_folder_path)


def convert_to_jpeg(base_file_path, jpeg_file_path, quality=95):
    try:
        base_image = image.open(base_file_path)
        base_image.convert("RGB").save(jpeg_file_path, "JPEG", quality=quality)
        
    except Exception as e:
        print(f"Error during conversion: {e}")
        
def convert_to_jpg(base_file_path, jpg_file_path, quality=95):
    try:
        base_image = image.open(base_file_path)
        base_image.convert("RGB").save(jpg_file_path, "JPEG", quality=quality)
        
    except Exception as e:
        print(f"Error during conversion: {e}")

def convert_png_to_jpeg(input_path, output_path):
    try:
        png_image = imageio.imread(input_path)

        imageio.imwrite(output_path, png_image[:, :, :3], format="JPEG", quality=95)

    except Exception as e:
        print(f"Error during conversion: {e}")
        

def change_date_taken(image_path, meta_date, meta_time):
    try:
        meta_datetime_str = f"{meta_date} {meta_time}"
        meta_datetime = datetime.strptime(meta_datetime_str, "%m-%d-%Y %H:%M:%S")
        formatted_date = meta_datetime.strftime("%Y:%m:%d %H:%M:%S")
        
        exif_dict = piexif.load(image_path)
        exif_dict['Exif'][piexif.ExifIFD.DateTimeOriginal] = formatted_date.encode('utf-8')
        
        exif_bytes = piexif.dump(exif_dict)
        piexif.insert(exif_bytes, image_path)

    except Exception as e:
        print(f"Error modifying metadata for {image_path}: {e}")
        
def send_error_email(error_value):
    sender_email = "abhaysankar.ecesis@gmail.com"
    password = "aaee uhow mwad oxgs"
    
    subject = "Propvision Error - AUTOMATED"
    body = "Error while executing program. Check immediatly! \n" + error_value
    to_email = "abhaysankar.ecesis@gmail.com"

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = to_email
    message["Subject"] = subject

    message.attach(MIMEText(body, "plain"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()

        server.login(sender_email, password)
        server.sendmail(sender_email, to_email, message.as_string())

    print("Email sent successfully!")

def get_exif_data(image_path):
    with image.open(image_path) as img:
        exif_data = img._getexif()
        if exif_data:
            for tag, value in exif_data.items():
                tag_name = TAGS.get(tag, tag)
                if tag_name == "DateTimeOriginal":
                    return value
                
def log_truncated(order_id):
    if not os.path.exists(truncated_file):
        open(truncated_file, 'a').close()
        
    with open(truncated_file, 'r') as file: 
        logged_orders = file.read().splitlines()
    
    if order_id not in logged_orders:
        logging.error(f'{order_id} has a truncated image')
        
        with open(truncated_file, 'a') as file:
            file.write(f"{order_id}\n")
            
        return True
    else:
        return False

def save_order_count(order_count, av_ordercount, date_checker, trunc_count):
    now = datetime.now()
    now_date = now.date()
    if date_checker < now_date:
        logging.info("Entered inside save_order_count function!!")

        date_test = date_checker.strftime('%m/%d/%y')
        date_test2 = date_checker.strftime('%Y-%m-%d')
        date_checker_str2 = str(date_test2)
        date_checker_str = str(date_test)

        order_count_data = {
        'Order Data' : date_checker_str,
        'Order_Count' : order_count,
        'Available_Orders' : av_ordercount
        }

        if not os.path.exists(ordercount_txt):
            open(ordercount_txt, 'a').close()
            
        with open(ordercount_txt, 'a') as file:
            file.write(f"{order_count_data}\n")

        like_date = date_checker_str + '%'

        verified_count_check = "SELECT COUNT(order_id) from pvdb_propvisiondb where status = 'Verified' and status_time LIKE %s"
        cursor.execute(verified_count_check, (like_date,))
        count_verified = cursor.fetchone()

        rejected_count_check = "SELECT COUNT(order_id) from pvdb_propvisiondb where status = 'Rejected' and status_time LIKE %s"
        cursor.execute(rejected_count_check, (like_date,))
        count_rejected = cursor.fetchone()

        # count_unverified_check = "SELECT COUNT(order_id) from pvdb_propvisiondb where status = 'Recieved' and pic_received like"
        count_unverified = (av_ordercount - (count_verified[0] + count_rejected[0]))

        manual_count_check = "SELECT COUNT(order_id) from pvdb_propvisiondb where status = 'Manual' and pic_received LIKE %s"
        date_picr = date_checker.strftime('%m-%d-%Y')
        date_picr_str = str(date_picr)

        like_date_picr = date_picr_str + '%'
        cursor.execute(manual_count_check, (like_date_picr,))
        manual_count = cursor.fetchone()

        if av_ordercount == 0:
            verification_rate = '0%'
        else:
            verification_rate = "{:.2f}%".format(((count_verified[0] + count_rejected[0]) / av_ordercount) * 100)

        data = [[date_checker_str, order_count, av_ordercount, trunc_count, str(count_verified[0]), str(count_rejected[0]), str(count_unverified),  str(manual_count[0]), str(verification_rate)]]
        worksheet = spreadsheet.worksheet('Daily Order Count')
        sheet_data = worksheet.get_all_values()
        if sheet_data:
            last_row = len(sheet_data) 
        else:
            last_row = 0
        sheet_order_num = last_row + 1
        worksheet.update(range_name = 'A{0}'.format(sheet_order_num),values = data)

        insert_query = "INSERT INTO pvdb_ordercount (date, order_count, trunc_count, count_verified, count_rejected, count_unverified, verification_rate, available_orders, manual_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,  %s)"

        cursor.execute(insert_query, (date_checker_str2, order_count, trunc_count, count_verified, count_rejected, count_unverified, verification_rate, av_ordercount, manual_count))
        logging.info(f'{date_checker_str} Order Count added to database')
        conn.commit()

        with open(recieved_json, 'r', encoding='utf-8') as file:
            received_json_data = json.load(file)

        order_idst = [item['OrderId'] for item in received_json_data['data']]
        order_ids = order_idst[0] if len(order_idst) == 1 else tuple(order_idst)

        if type(order_ids) == int:
             sql_query = f"""
                    SELECT COUNT(order_id) FROM pvdb_propvisiondb
                    WHERE status = 'Received'
                    AND order_id = {order_ids}
                """
        else:
            sql_query = f"""
                        SELECT COUNT(order_id) FROM pvdb_propvisiondb
                        WHERE status = 'Received'
                        AND order_id IN {order_ids}
                    """

        cursor.execute(sql_query,)
        base_count = cursor.fetchone()[0]
            
        order_count = base_count 

        midnight_count_data = {
        'Avaiable Orders_Midnight' : order_count,
        }

        if not os.path.exists(ordercount_txt):
            open(ordercount_txt, 'a').close()
            
        with open(ordercount_txt, 'a') as file:
            file.write(f"{midnight_count_data}\n")


        order_count_data = {
            'Last_Order_Update' : date_checker_str,
            'Order_Count' : order_count,
            'Available_Orders': av_ordercount,
            "Trunc_Count" : trunc_count,
        }

        with open(live_ordercount_file, 'w') as file:
            json.dump(order_count_data, file, indent=4)

        logging.info("Resetting Order Count!")
        return order_count

def update_date(date_checker):
    now = datetime.now().date()
    if date_checker < now:
        return now
    return date_checker

def update_trc(date_checker, trunc_count):
    now = datetime.now().date()
    if date_checker < now:
        logging.info("Resetting Trunc Count!")
        return 0
    return trunc_count

def restart_program():
    python_exe = sys.executable
    script_path = sys.argv[0]
    script_args = sys.argv[1:]

    message = f"Executing: {python_exe} {script_path} {' '.join(script_args)}"  

    try:
        subprocess.run([python_exe, script_path] + script_args, check=True)
    except subprocess.CalledProcessError as e:
        error_str = f"Error restarting the program: {e}"
        logging.error(error_str)
        # sys.exit(1)
    finally:
        logging.info(message)


def compress_image(base_file_path, output_file_path, max_size, quality=95):
    try:
        file_size = os.path.getsize(base_file_path)
        
        if file_size <= max_size:
            return
        else:
            logging.info(f"{base_file_path} is not under {max_size}.")
        
        img = image.open(base_file_path)
        
        while file_size > max_size and quality > 10:
            img.save(output_file_path, "JPEG", quality=quality)
            file_size = os.path.getsize(output_file_path)
            
            quality -= 5

        if file_size <= max_size:
            logging.info(f"{base_file_path} Image successfully compressed")
        else:
            logging.error(f"Couldn't Compress {base_file_path}.")
    except Exception as e:
        logging.error(f"Compression Error: {e}")


run_number = 0
test_flag = False
img_test = ''
now_date = 0
days = []
completed_array = []
now = datetime.now()
date_checker = now.date()
live_ordercount_file = 'C:\\Users\\USER\\Downloads\\Test_Folder1\\order_data.json'
ordercount_txt = 'C:\\Users\\USER\\Downloads\\Test_Folder1\\ordercount_data.txt'
naming_json = 'C:\\Users\\USER\\Downloads\\Test_Folder1\\naming.json'



ordertype_values = {
    2: "EXT",
    3: "Rental",
    4: "PCR",
    5: "Inspection",
    11: "EXT + RNT",
    12: "EXT + RNT + INP",
    13: "EXT + INP",
    17: "EXT PC",
    18: "EXT PI",
    19: "MMR",
    20: "Desktop BPO"
}

if (os.path.isfile(live_ordercount_file)):
    with open(live_ordercount_file, 'r') as file:
        json_data = json.loads(file.read())

    order_count = json_data['Order_Count']
    trunc_count = json_data['Trunc_Count']
    av_ordercount = json_data['Available_Orders']

else:
    order_count = 0
    trunc_count = 0
    av_ordercount = 0

try:    
    while(True):
        run_number += 1
        print('Number of runs: ',run_number)

        check_oc = save_order_count(order_count, av_ordercount, date_checker, trunc_count)

        if check_oc is not None:
            av_ordercount = check_oc
            order_count = 0

        trunc_count = update_trc(date_checker, trunc_count)

        if(update_date(date_checker)):
            date_checker = update_date(date_checker)

        subj_addr = []

        queue_status_path = 'C:\\Users\\USER\\Downloads\\Test_Folder1\\queue_status.json'
        queuejson_path = 'C:\\Users\\USER\\Downloads\\Test_Folder1\\queuejson.json'

        with open(queue_status_path, 'r') as file:
                data = json.loads(file.read())

        queue_status = data['status']

        if queue_status == 'True':
            select_query = 'SELECT id, folder_path FROM order_queue'

            cursor.execute(select_query,)
            queue = cursor.fetchall()
            
            queue_addr = {row[0]: row[1] for row in queue}

            keys = list(queue_addr.keys())
            keys_tuple = keys[0] if len(keys) == 1 else tuple(keys)

            queuejson_data = {
                row[0]: row[1] for row in queue
                }

            with open(queuejson_path, 'w') as file:
                json.dump(queuejson_data, file, indent=4)


            if type(keys_tuple) != int:
                 if len(keys_tuple) == 0:
                     time.sleep(60)
                     logging.error('Program Restarting since No Order In Queue!!')
                     restart_program()
            

            if type(keys_tuple) == int:
                new_select_query = f'DELETE from order_queue where id = {keys_tuple} '
                print('query', new_select_query)
                cursor.execute(new_select_query, )
                conn.commit()
            else: 
                new_select_query = f'DELETE from order_queue where id in {keys_tuple} '
                print('query', new_select_query)
                cursor.execute(new_select_query, )
                conn.commit()

            status_true = {"status": "False"}
            json_data = json.dumps(status_true, indent=4)
            with open(queue_status_path, 'w', encoding='utf-8') as file:
                file.write(json_data)

            all_files = []

            jpg_extensions = ('.jpg', '.jpeg', '.JPG')
            png_extension = '.png'
            base_folder_length = 0

            try:
                for keys in queue_addr:
                    directory = queue_addr[keys]
                    for files in os.listdir(directory):
                        empty_check = os.listdir(directory)
                        for item in empty_check:
                            if len(empty_check) == 1 and item == 'client_detail.json':
                                continue                         
                            # elif 'Completed' in item:
                            #     with open(os.path.join(directory, 'client_detail.json'), 'r') as file:
                            #         data = json.loads(file.read())
                            #     order_id = data['Order_ID']
                            #     address = data['Address']
                            #     status = 'Completed'
                            #     update_query = "UPDATE pvdb_propvisiondb SET status = %s WHERE order_id = %s"
                            #     cursor.execute(update_query, (status, order_id))
                            #     if address not in completed_array:
                            #         logging.info(f'Status changed from Verified to Completed for {address}')
                            #         completed_array.append(address)
                            #     conn.commit()
                            elif ('client_detail.json' in item):
                                with open(os.path.join(directory, 'client_detail.json'), 'r') as file:
                                    data = json.loads(file.read())
                                order_type = data['order_type']
                                order_id = data['order_id']

                                select_query = "SELECT * FROM prop_test WHERE order_id = %s" 
                                
                                cursor.execute(select_query, (order_id,))
                                existing_record = cursor.fetchone()

                                if not os.path.exists(truncated_file):
                                    open(truncated_file, 'a').close()
                                with open(truncated_file, 'r') as file:
                                    trunc_orders = file.read().splitlines()

                                if existing_record:
                                    continue
                                elif directory in trunc_orders :
                                    continue
                                elif order_type in ordertype_values and directory not in subj_addr:
                                    subj_addr.append(directory)
                                    logging.info(f'{directory} added to subj_addr array')
                                else:
                                    continue
                            else:
                                continue
            except Exception as e:
                print(f'Error: {e}')

            print('subj_addr: ', subj_addr)

            for i in range(0, len(subj_addr)):
                new_dir = subj_addr[i]
                json_path = subj_addr[i] + '\\client_detail.json'
                copy_path = new_dir + '\\RAW'
                move_path = new_dir + '\\model'
                img_flag = False
                suffix = ('.jpg', '.jpeg', '.JPG', '.png')
                image_addr = {}
                all_files1 = os.listdir(new_dir)

                # time.sleep(5)

                logging.info(f'Base files in {new_dir}: {all_files1} ')

                base_length = len(all_files1)
                if 'Thumbs.db' in all_files1:
                    base_length -=1

                matching_files = [file for file in all_files1 if file.endswith(suffix)]
                logging.info(f'Matching Files : {matching_files}')
                counter = 1

                for file_name in matching_files:
                    image_addr[counter] = os.path.join(new_dir, file_name)
                    counter += 1

                create_raw_folder(subj_addr[i])
                create_model_folder(subj_addr[i])

      
                base_folder_length += 1
                for file_name in matching_files: 
                    if not os.path.exists(truncated_file):
                        open(truncated_file, 'a').close()
                    with open(truncated_file, 'r') as file:
                        trunc_orders = file.read().splitlines()
                    if new_dir not in trunc_orders:
                        source_file_path = os.path.join(new_dir, file_name)
                        destination_raw_file_path = os.path.join(copy_path, file_name)
                        destination_model_file_path = os.path.join(move_path, file_name)

                        try:
                            shutil.copy2(source_file_path, destination_raw_file_path)
                            logging.info(f'Copying {file_name} to {destination_raw_file_path}')
                        except Exception as e:
                            print(f'Error copying file "{file_name}": {e}')
                            logging.error(f'Copying Error occurred: {str(e)}')

                        try:
                            shutil.move(source_file_path, destination_model_file_path)
                            logging.info(f'Moving {file_name} to {destination_model_file_path}')
                        except Exception as e:
                            print(f'Error moving file "{file_name}": {e}')
                            logging.error(f'Moving Error occurred: {str(e)}')
                    else:
                        continue


            pred_val1 = {}
            pred_val2 = {}
            pred_val3 = {}
            preds_1 = {}
            preds_2 = {}
            preds_3 = {}
            processed_images = []
            ass_processed_images = []
            folder_count = 0

            def get_key(dictionary, value):
                for key, val in dictionary.items():
                    if val == value:
                        return key  
                return None  


            for j in range(0, len(subj_addr)):
                img_flag = False
                sheet_flag = True
                base_path = subj_addr[j]
                json_path1 = subj_addr[j] + '\\client_detail.json'
                new_dir = subj_addr[j] + '\\model'
                new_dir1 = subj_addr[j] + '\\model'
                raw_path = subj_addr[j] + '\\RAW'
                new_dir = os.path.join(new_dir + '\\')
                suffix = ('.jpg', '.jpeg', '.JPG', '.png')
                image_paths1 = {}
                new_renamed_paths ={}
                new_renamed_paths1 = {}
                try:
                    all_files = os.listdir(new_dir)
                except:
                    continue

                matching_files = [file for file in all_files if file.endswith(suffix)]

                counter = 1
                for file_name in matching_files:
                    image_paths1[counter] = os.path.join(new_dir, file_name)
                    counter += 1
                
                print('image_paths1: ', image_paths1)

                for i in range(1, len(image_paths1) + 1):
                    img_path = image_paths1[i]
                    try:
                        img = image.open(img_path).resize((224, 224))
                    except Exception as e:
                        img_flag = True
                        logging.error(f'Image Loading Error: {str(e)} for {new_dir}')
                        continue

                if img_flag == True:
                    check_trc = log_truncated(base_path)
                    if check_trc:
                        trunc_count+=1
                    if os.path.exists(new_dir1):
                        shutil.rmtree(new_dir1)
                        raw_files = os.listdir(raw_path)

                        for raw_images in raw_files:
                            source_raw_file_path = os.path.join(raw_path,raw_images)
                            try:
                                shutil.move(source_raw_file_path, base_path)
                            except Exception as e:
                                print(f'Error moving file "{file_name}": {e}')
                        shutil.rmtree(raw_path)
                        continue
                    else:
                        logging.error('Truncated Folder Deletion Issue')


                address = ''
                status = "Received"
                assigned_val = 'Not Assigned'
                inuse_value = False
                
                subj_path = os.path.join(subj_addr[j], 'model')

                with open(json_path1, 'r') as file:
                    data = json.loads(file.read())

                portal = data['portal_name']
                address = data['subject_address']
                base_order_date = data['order_date']
                base_order_date = base_order_date.split('.')[0]
                base_due_date = data['due_date']
                order_id = data['order_id']
                order_type_base = data['order_type']
                order_type = ordertype_values[order_type_base]
                client = data['client_name']
                base_pic_recvd_time = data['Pic_Recieved_date']
                order_instruct = data['instructions']
                subclient_id = data['sub_client_id']
                subclient_full = data['subclient']

                try:
                    sub_client = subclient_full.split('_')[1]
                except:
                    sub_client = subclient_full

                try:
                    datetime_obj2 = datetime.strptime(base_due_date, "%Y-%m-%dT%H:%M:%S")
                    due_date = datetime_obj2.strftime("%m-%d-%Y %H:%M:%S")
                except:
                    datetime_obj2 = datetime.strptime(base_due_date, "%Y-%m-%dT%H:%M:%S.%f")
                    due_date = datetime_obj2.strftime("%m-%d-%Y %H:%M:%S")


                try:
                    datetime_obj = datetime.strptime(base_order_date, "%Y-%m-%dT%H:%M:%S")
                    order_date = datetime_obj.strftime("%m-%d-%Y %H:%M:%S")
                except:
                    datetime_obj = datetime.strptime(base_order_date, "%Y-%m-%dT%H:%M:%S.%f")
                    order_date = datetime_obj.strftime("%m-%d-%Y %H:%M:%S")


                try:
                    datetime_obj1 = datetime.strptime(base_pic_recvd_time, "%Y-%m-%dT%H:%M:%S.%f")
                    pic_recvd_time = datetime_obj1.strftime("%m-%d-%Y %H:%M:%S")

                except:
                    datetime_obj1 = datetime.strptime(base_pic_recvd_time, "%Y-%m-%dT%H:%M:%S")
                    pic_recvd_time = datetime_obj1.strftime("%m-%d-%Y %H:%M:%S")


                select_query = "SELECT * FROM prop_test WHERE order_id = %s"
                insert_query = "INSERT INTO prop_test (client_name, subject_address, status, folder_path, assignedto, subclient_name, portal_name, due_date, order_id, order_type, order_date, in_use, order_instructions, pic_recieved, subclient_id, subclient_full) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                cursor.execute(select_query, (order_id,))
                existing_record = cursor.fetchone() 

                if existing_record:
                    continue
                else:
                    cursor.execute(insert_query, (client, address, status, subj_path, assigned_val, sub_client, portal, due_date, order_id, order_type, order_date, inuse_value,order_instruct, pic_recvd_time, subclient_id, subclient_full))
                    print(f"Inserted '{address}' into the database.")
                    logging.info(f'{address} added to database')
                conn.commit()


                count_sv = 1
                count_t2_sv = ''
                count_rsv = ''
                count_lsv = ''
                count_lrv = 1
                count_vc_lrv = ''
                count_rv = ''
                count_lv = ''
                count_fv = ''
                count_ss = ''
                count_av = ''
                result = ''
                count_o = ''

                if image_paths1 == '':
                    continue
                else:
                    with open(naming_json, 'r') as file:
                        naming_data = json.loads(file.read())

                    logging.info(f'Portal is {portal}')
                    
                    if portal in naming_data:
                        logging.info(f'Entered first naming data')
                        names = naming_data[portal]
                        Subject_Front = names['Subject Front']
                        Subject_Left = names['Subject Left']
                        Subject_Right = names['Subject Right']
                        Subject_Street = names['Subject Street']
                        Subject_Street1 = names['Subject Street 1']
                        Subject_Back = names['Subject Back']
                        Subject_Street_Sign = names ['Subject Street Sign']
                        Subject_Address_Verification = names['Subject Address Verification']
                        SView_Type = names['SView_Type']
                    else:
                        logging.info(f'Entered second naming data')
                        names = naming_data['RED BELL']
                        Subject_Front = names['Subject Front']
                        Subject_Left = names['Subject Left']
                        Subject_Right = names['Subject Right']
                        Subject_Street = names['Subject Street']
                        Subject_Street1 = names['Subject Street 1']
                        Subject_Back = names['Subject Back']
                        Subject_Street_Sign = names ['Subject Street Sign']
                        Subject_Address_Verification = names['Subject Address Verification']
                        SView_Type = names['SView_Type']

                    for j in range(1, len(image_paths1) + 1):
                        file_extension = os.path.splitext(image_paths1[j])[1]
                        path1 = pathlib.Path(image_paths1[j])
                        img_path = image_paths1[j]

                        print('img_path: ',img_path)

                        try:
                            img = image.open(img_path).resize((224, 224))
                        except Exception as e:
                            logging.error(f'Image Loading Error: {str(e)}')
                            continue

                        x = np.array(img)
                        x = np.expand_dims(x, axis=0)
                        x = preprocess_input(x)  

                        mpred1 = model1.predict(x)
                        pred_val1[image_paths1[j]] = mpred1
                        mpred2 = model2.predict(x)
                        pred_val2[image_paths1[j]] = mpred2
                        mpred3 = model3.predict(x)
                        pred_val3[image_paths1[j]] = mpred3

                        logging.info(f'{img_path} run through the model')

                        pred1 = np.argmax(mpred1)
                        pred2 = np.argmax(mpred2)
                        pred3 = np.argmax(mpred3)

                        preds_1[image_paths1[j]] = pred1
                        preds_2[image_paths1[j]] = pred2
                        preds_3[image_paths1[j]] = pred3

                    if sheet_flag == True:
                        current_datetime_main = datetime.now()
                        pic_recvd_time_dt = datetime.strptime(pic_recvd_time, "%m-%d-%Y %H:%M:%S")
                        time_difference = current_datetime_main - pic_recvd_time_dt
                        time_difference_str = str(time_difference)
                        current_datetime = str(current_datetime_main)
                        data = [[str(address),str(new_dir), str(client), str(sub_client), str(order_type), str(portal), str(pic_recvd_time), current_datetime, time_difference_str]]
                        if portal == 'RED BELL':
                            worksheet = spreadsheet.worksheet('Red Bell Orders')
                            sheet_data = worksheet.get_all_values()
                            if sheet_data:
                                last_row = len(sheet_data) 
                            else:
                                last_row = 0
                            sheet_order_num = last_row + 1
                            worksheet.update(range_name = 'A{0}'.format(sheet_order_num),values = data)
                            sheet_flag = False

                        else:
                            worksheet = spreadsheet.worksheet('Other Portal Orders')
                            sheet_data = worksheet.get_all_values()
                            if sheet_data:
                                last_row = len(sheet_data) 
                            else:
                                last_row = 0
                            sheet_order_num = last_row+1
                            worksheet.update(range_name = 'A{0}'.format(sheet_order_num),values = data)
                            sheet_flag = False                           
                    else:
                        continue


                    order_count+=1
                    av_ordercount+=1
                    now = datetime.now()
                    now_str = str(now)

                    order_count_data = {
                        'Last_Order_Update' : now_str,
                        'Order_Count' : order_count,
                        'Available_Orders': av_ordercount,
                        "Trunc_Count" : trunc_count,
                    }

                    with open(live_ordercount_file, 'w') as file:
                        json.dump(order_count_data, file, indent=4)

                    all_preds = [preds_1, preds_2, preds_3]

                    count_1_p1 = sum(1 for i in preds_1.values() if i == 1)
                    count_2_p1 = sum(1 for i in preds_1.values() if i == 2)
                    count_3_p1 = sum(1 for i in preds_1.values() if i == 3)

                    count_1_p2 = sum(1 for i in preds_2.values() if i == 1)
                    count_2_p2 = sum(1 for i in preds_2.values() if i == 2)
                    count_3_p2 = sum(1 for i in preds_2.values() if i == 3)

                    count_1_p3 = sum(1 for i in preds_3.values() if i == 1)
                    count_2_p3 = sum(1 for i in preds_3.values() if i == 2) 
                    count_3_p3 = sum(1 for i in preds_3.values() if i == 3)

                    def ass(count_av, count_ss, count_vc_lrv, count_sv,count_rv, count_lrv,count_fv, count_rsv, count_lsv, count_t2_sv):
                        for key in preds_1:
                            if key in preds_2 and key in preds_3:
                                if key not in processed_images:
                                    pred1 = pred_val1[key]
                                    pred2 = pred_val2[key]
                                    pred3 = pred_val3[key]

                                    arg_arr = []
                                    arg_arr.append(np.argmax(pred1))
                                    arg_arr.append(np.argmax(pred2))
                                    arg_arr.append(np.argmax(pred3))

                                    for val in arg_arr:
                                        result = int((np.argmax((pred1+pred2+pred3)/3)))
                                        if key not in ass_processed_images:
                                            ass_processed_images.append(key)
                                            try:
                                                if result == 0:
                                                    path_a = pathlib.Path(key)
                                                    if count_av == '':
                                                        path_a.rename(new_dir + Subject_Address_Verification + file_extension)
                                                        logging.info(f'Renamed {path_a} to {Subject_Address_Verification}')
                                                        count_av = 1
                                                    else:
                                                        path_a.rename(new_dir + Subject_Address_Verification +  "_{0}".format(count_av) + file_extension)
                                                        logging.info(f'Renamed {path_a} to {Subject_Address_Verification}_{count_av}')
                                                        count_av += 1                                                     
                                                elif result == 1:
                                                    path_fv = pathlib.Path(key)
                                                    if count_fv == '':
                                                        path_fv.rename(new_dir + Subject_Front + file_extension)
                                                        logging.info(f'Renamed {path_fv} to {Subject_Front}')
                                                        count_fv = 1
                                                    else:
                                                        path_fv.rename(new_dir + Subject_Front +"_{0}".format(count_fv)+ file_extension)
                                                        logging.info(f'Renamed {path_fv} to {Subject_Front}_{count_fv}')
                                                        count_fv += 1
                                                elif result == 2:
                                                    path_lv = pathlib.Path(key)
                                                    if portal == 'RED BELL' or portal not in naming_data:
                                                        path_lv.rename(new_dir + Subject_Left +"_{0}".format(count_lrv)+ file_extension)
                                                        logging.info(f"Renamed {path_lv} to {Subject_Left}_{count_lrv}")
                                                        count_lrv += 1
                                                    elif portal == 'Valuation Connect':
                                                        if count_vc_lrv == '':
                                                            path_lv.rename(new_dir + Subject_Left + file_extension)
                                                            logging.info(f'Renamed {path_lv} to {Subject_Left}')
                                                            count_vc_lrv = 1 
                                                        else:
                                                            path_lv.rename(new_dir + Subject_Left +"_{0}".format(count_vc_lrv)+ file_extension)
                                                            logging.info(f"Renamed {path_lv} to {Subject_Left}_{count_lrv}")
                                                            count_vc_lrv += 1
                                                    else:
                                                        if count_lv == '':
                                                            path_lv.rename(new_dir + Subject_Left + file_extension)
                                                            logging.info(f'Renamed {path_lv} to {Subject_Left}')
                                                            count_lv = 1 
                                                        else:
                                                            path_lv.rename(new_dir + Subject_Left +"_{0}".format(count_lv)+ file_extension)
                                                            logging.info(f'Renamed {path_lv} to {Subject_Left}_{count_lv}')
                                                            count_lv += 1

                                                elif result == 3:
                                                    path_rv = pathlib.Path(key)
                                                    if portal == 'RED BELL' or portal not in naming_data:
                                                        path_rv.rename(new_dir + Subject_Right +"_{0}".format(count_lrv)+ file_extension)
                                                        logging.info(f"Renamed {path_rv} to {Subject_Right}_{count_lrv}")
                                                        count_lrv += 1
                                                    elif portal == 'Valuation Connect':
                                                        if count_vc_lrv == '':
                                                            path_rv.rename(new_dir + Subject_Right + file_extension)
                                                            logging.info(f'Renamed {path_rv} to {Subject_Right}')
                                                            count_vc_lrv = 1
                                                        else:
                                                            path_rv.rename(new_dir + Subject_Right +"_{0}".format(count_vc_lrv)+ file_extension)
                                                            logging.info(f"Renamed {path_rv} to {Subject_Right}_{count_vc_lrv}")
                                                            count_vc_lrv += 1
                                                    else:
                                                        if count_rv == '':
                                                            path_rv.rename(new_dir + Subject_Right + file_extension)
                                                            logging.info(f'Renamed {path_rv} to {Subject_Right}')
                                                            count_rv = 1
                                                        else:
                                                            path_rv.rename(new_dir + Subject_Right + "_{0}".format(count_rv)+ file_extension)
                                                            logging.info(f'Renamed {path_rv} to {Subject_Right}_{count_rv}')
                                                            count_rv += 1
                                                elif result == 4:
                                                    path_ss = pathlib.Path(key) 
                                                    if count_ss == '':
                                                        path_ss.rename(new_dir + Subject_Street_Sign + file_extension)
                                                        logging.info(f'Renamed {path_ss} to {Subject_Street_Sign}')
                                                        count_ss = 1
                                                    else:
                                                        path_ss.rename(new_dir + Subject_Street_Sign +"_{0}".format(count_ss) + file_extension)
                                                        logging.info(f"Renamed {path_ss} to {Subject_Street_Sign}_{count_ss}")
                                                        count_ss += 1
                                                elif result ==5:
                                                    path_sv = pathlib.Path(key)
                                                    if SView_Type == 'Type-1':
                                                        logging.info('Type 1 Street Entered')
                                                        path_sv.rename(new_dir + Subject_Street  +"_{0}".format(count_sv)+ file_extension)
                                                        logging.info(f"Renamed {path_sv} to {Subject_Street}_{count_sv}")
                                                        count_sv  += 1
                                                    elif SView_Type == 'Type-2':
                                                        logging.info('Type 2 Street Entered')
                                                        if count_t2_sv == '':
                                                            path_sv.rename(new_dir + Subject_Street + file_extension)
                                                            logging.info(f"Renamed {path_sv} to {Subject_Street}_{count_t2_sv}")
                                                            count_t2_sv = 1
                                                        else:
                                                            path_sv.rename(new_dir + Subject_Street +"_{0}".format(count_t2_sv) + file_extension)
                                                            logging.info(f"Renamed {path_sv} to {Subject_Street}_{count_t2_sv}")
                                                            count_t2_sv += 1
                                                    elif SView_Type == 'Type-3':
                                                        logging.info('Type 3 Street Entered')
                                                        if count_sv % 2 != 0:
                                                            if count_rsv == '':
                                                                path_sv.rename(new_dir + Subject_Street + file_extension)
                                                                logging.info(f'Renamed {path_sv} to {Subject_Street}') 
                                                                count_rsv = 1
                                                                count_sv += 1
                                                            else:
                                                                path_sv.rename(new_dir + Subject_Street +"_{0}".format(count_rsv) + file_extension)
                                                                logging.info(f"Renamed {path_sv} to {Subject_Street}_{count_rsv}")
                                                                count_rsv += 1
                                                                count_sv += 1
                                                        else:
                                                            if count_lsv == '':
                                                                path_sv.rename(new_dir + Subject_Street1 + file_extension)
                                                                logging.info(f'Renamed {path_sv} to {Subject_Street1}') 
                                                                count_lsv = 1
                                                                count_sv += 1
                                                            else:
                                                                path_sv.rename(new_dir + Subject_Street1 +"_{0}".format(count_lsv) + file_extension)
                                                                logging.info(f"Renamed {path_sv} to {Subject_Street1}_{count_lsv}")
                                                                count_lsv += 1
                                                                count_sv += 1
                                                    elif SView_Type == 'Type-4':
                                                        logging.info('Type 4 Street Entered')
                                                        path_sv.rename(new_dir + Subject_Street  +"{0}".format(count_sv)+ file_extension)
                                                        logging.info(f"Renamed {path_sv} to {Subject_Street}_{count_sv}")
                                                        count_sv  += 1
                                                else:
                                                    path_o = pathlib.Path(key) 
                                                    if count_o == '':
                                                        path_o.rename(new_dir + "Other" + file_extension)
                                                        logging.info(f'Renamed {path_o} to Other')
                                                        count_o = 1
                                                    else:
                                                        path_o.rename(new_dir + "Other_{0}".format(count_o) + file_extension)
                                                        logging.info(f"Renamed {path_o} to Other{count_o}")
                                                        count_o +=1
                                            except Exception as e:
                                                logging.info(f'Duplicate Error 1 {str(e)}')
                                                continue
                                        else:
                                            continue

                    if count_1_p1 == 1 and count_2_p1 == 1 and count_3_p1 == 1:
                            try:
                                if SView_Type == 'Type-1':
                                    path_1 = pathlib.Path(get_key(preds_1, 1))
                                    path_1.rename(new_dir + Subject_Front + file_extension)
                                    logging.info(f'Renamed {path_1} to {Subject_Front}')
                                    processed_images.append(get_key(preds_1, 1))
                                    path_2 = pathlib.Path(get_key(preds_1, 2))
                                    path_2.rename(new_dir + Subject_Left + '_1' + file_extension)
                                    logging.info(f'Renamed {path_2} to {Subject_Left}')
                                    processed_images.append(get_key(preds_1, 2))
                                    path_3 = pathlib.Path(get_key(preds_1, 3))
                                    path_3.rename(new_dir + Subject_Right + '_2'+file_extension)
                                    logging.info(f'Renamed {path_3} to {Subject_Right}')
                                    processed_images.append(get_key(preds_1, 3))
                                    count_lrv=3
                                    count_fv=1
                                    ass(count_av, count_ss, count_vc_lrv, count_sv,count_rv, count_lrv,count_fv, count_rsv, count_lsv, count_t2_sv)
                                else:
                                    path_1 = pathlib.Path(get_key(preds_1, 1))
                                    path_1.rename(new_dir + Subject_Front + file_extension)
                                    logging.info(f'Renamed {path_1} to {Subject_Front}')
                                    processed_images.append(get_key(preds_1, 1))
                                    path_2 = pathlib.Path(get_key(preds_1, 2))
                                    path_2.rename(new_dir + Subject_Left + file_extension)
                                    logging.info(f'Renamed {path_2} to {Subject_Left}')
                                    processed_images.append(get_key(preds_1, 2))
                                    if portal == 'Valuation Connect':
                                        path_3 = pathlib.Path(get_key(preds_1, 3))
                                        path_3.rename(new_dir + Subject_Right +"_1" + file_extension)
                                        logging.info(f'Renamed {path_3} to {Subject_Right}')
                                        processed_images.append(get_key(preds_1, 3))
                                        count_lrv=3
                                        count_fv=1
                                    else:
                                        path_3 = pathlib.Path(get_key(preds_1, 3))
                                        path_3.rename(new_dir + Subject_Right + file_extension)
                                        logging.info(f'Renamed {path_3} to {Subject_Right}')
                                        processed_images.append(get_key(preds_1, 3))
                                        count_lrv=3
                                        count_fv=1
                                    ass(count_av, count_ss, count_vc_lrv, count_sv,count_rv, count_lrv,count_fv, count_rsv, count_lsv, count_t2_sv)
                            except Exception as e:
                                logging.info(f'Duplicate Error 2 {str(e)}')
                                continue
                    elif count_1_p2 == 1 and count_2_p2 == 1 and count_3_p2 == 1:
                            try:
                                if SView_Type == 'Type-1':
                                    path_1 = pathlib.Path(get_key(preds_2, 1))
                                    path_1.rename(new_dir + Subject_Front + file_extension)
                                    logging.info(f'Renamed {path_1} to {Subject_Front}')
                                    processed_images.append(get_key(preds_2, 1))
                                    path_2 = pathlib.Path(get_key(preds_2, 2))
                                    path_2.rename(new_dir + Subject_Left + '_1' +file_extension)
                                    logging.info(f'Renamed {path_2} to {Subject_Left}')
                                    processed_images.append(get_key(preds_2, 2))
                                    path_3 = pathlib.Path(get_key(preds_2, 3))
                                    path_3.rename(new_dir + Subject_Right + '_2' +file_extension)
                                    logging.info(f'Renamed {path_3} to {Subject_Right}')
                                    processed_images.append(get_key(preds_2, 3))
                                    count_lrv=3
                                    count_fv=1
                                    ass(count_av, count_ss, count_vc_lrv, count_sv,count_rv, count_lrv,count_fv, count_rsv, count_lsv, count_t2_sv)
                                else:
                                    path_1 = pathlib.Path(get_key(preds_2, 1))
                                    path_1.rename(new_dir + Subject_Front + file_extension)
                                    logging.info(f'Renamed {path_1} to {Subject_Front}')
                                    processed_images.append(get_key(preds_2, 1))
                                    path_2 = pathlib.Path(get_key(preds_2, 2))
                                    path_2.rename(new_dir + Subject_Left + file_extension)
                                    logging.info(f'Renamed {path_2} to {Subject_Left}')
                                    processed_images.append(get_key(preds_2, 2))
                                    if portal == 'Valuation Connect':
                                        path_3 = pathlib.Path(get_key(preds_1, 3))
                                        path_3.rename(new_dir + Subject_Right +"_1" + file_extension)
                                        logging.info(f'Renamed {path_3} to {Subject_Right}')
                                        processed_images.append(get_key(preds_1, 3))
                                        count_lrv=3
                                        count_fv=1
                                    else:
                                        path_3 = pathlib.Path(get_key(preds_1, 3))
                                        path_3.rename(new_dir + Subject_Right + file_extension)
                                        logging.info(f'Renamed {path_3} to {Subject_Right}')
                                        processed_images.append(get_key(preds_1, 3))
                                        count_lrv=3
                                        count_fv=1
                                    ass(count_av, count_ss, count_vc_lrv, count_sv,count_rv, count_lrv,count_fv, count_rsv, count_lsv, count_t2_sv)
                            except Exception as e:
                                logging.info(f'Duplicate Error 3 {str(e)}')
                                continue
                    elif count_1_p3 == 1 and count_2_p3 == 1 and count_3_p3 == 1:
                            try:
                                if SView_Type == 'Type-1':
                                    path_1 = pathlib.Path(get_key(preds_3, 1))
                                    path_1.rename(new_dir + Subject_Front + file_extension)
                                    logging.info(f'Renamed {path_1} to {Subject_Front}')
                                    processed_images.append(get_key(preds_3, 1))
                                    path_2 = pathlib.Path(get_key(preds_3, 2))
                                    path_2.rename(new_dir + Subject_Left+ '_1' + file_extension)
                                    logging.info(f'Renamed {path_2} to {Subject_Left}')
                                    processed_images.append(get_key(preds_3, 2))
                                    path_3 = pathlib.Path(get_key(preds_3, 3))
                                    path_3.rename(new_dir + Subject_Right + '_2' +file_extension)
                                    logging.info(f'Renamed {path_3} to {Subject_Right}')
                                    processed_images.append(get_key(preds_3, 3))
                                    count_lrv=3
                                    count_fv=1
                                    ass(count_av, count_ss, count_vc_lrv, count_sv,count_rv, count_lrv,count_fv, count_rsv, count_lsv, count_t2_sv)
                                else:
                                    path_1 = pathlib.Path(get_key(preds_3, 1))
                                    path_1.rename(new_dir + Subject_Front + file_extension)
                                    logging.info(f'Renamed {path_1} to {Subject_Front}')
                                    processed_images.append(get_key(preds_3, 1))
                                    path_2 = pathlib.Path(get_key(preds_3, 2))
                                    path_2.rename(new_dir + Subject_Left + file_extension)
                                    logging.info(f'Renamed {path_2} to {Subject_Left}')
                                    processed_images.append(get_key(preds_3, 2))
                                    if portal == 'Valuation Connect':
                                        path_3 = pathlib.Path(get_key(preds_1, 3))
                                        path_3.rename(new_dir + Subject_Right +"_1" + file_extension)
                                        logging.info(f'Renamed {path_3} to {Subject_Right}')
                                        processed_images.append(get_key(preds_1, 3))
                                        count_lrv=3
                                        count_fv=1
                                    else:
                                        path_3 = pathlib.Path(get_key(preds_1, 3))
                                        path_3.rename(new_dir + Subject_Right + file_extension)
                                        logging.info(f'Renamed {path_3} to {Subject_Right}')
                                        processed_images.append(get_key(preds_1, 3))
                                        count_lrv=3
                                        count_fv=1
                                    ass(count_av, count_ss, count_vc_lrv, count_sv,count_rv, count_lrv,count_fv, count_rsv, count_lsv, count_t2_sv)
                            except Exception as e:
                                logging.info(f'Duplicate Error 4 {str(e)}')
                                continue
                    else:
                        for key in preds_1:
                            if key in preds_2 and key in preds_3:

                                pred1 = pred_val1[key]
                                pred2 = pred_val2[key]
                                pred3 = pred_val3[key]
                                avg_prediction = int((np.argmax((pred1+pred2+pred3)/3)))

                                try:
                                    if avg_prediction == 0:
                                        path_a = pathlib.Path(key)
                                        if count_av == '':
                                            path_a.rename(new_dir + Subject_Address_Verification + file_extension)
                                            logging.info(f'Renamed {path_a} to {Subject_Address_Verification}')
                                            count_av = 1
                                        else:
                                            path_a.rename(new_dir + Subject_Address_Verification + "_{0}".format(count_av) + file_extension)
                                            logging.info(f'Renamed {path_a} to {Subject_Address_Verification}_{count_av}')
                                            count_av += 1
                                    elif avg_prediction == 1:
                                        path_fv = pathlib.Path(key)
                                        if count_fv == '':
                                            path_fv.rename(new_dir + Subject_Front + file_extension)
                                            logging.info(f'Renamed {path_fv} to {Subject_Front}')
                                            count_fv = 1
                                        else:
                                            path_fv.rename(new_dir + Subject_Front + "_{0}".format(count_fv)+ file_extension)
                                            logging.info(f'Renamed {path_fv} to {Subject_Front}_{count_fv}')
                                            count_fv += 1
                                    elif avg_prediction == 2:
                                        path_lv = pathlib.Path(key)
                                        if portal == 'RED BELL' or portal not in naming_data:
                                            path_lv.rename(new_dir + Subject_Left +"_{0}".format(count_lrv)+ file_extension)
                                            logging.info(f"Renamed {path_lv} to {Subject_Left}_{count_lrv}")
                                            count_lrv += 1
                                        elif portal == 'Valuation Connect':
                                            if count_vc_lrv == '':
                                                path_lv.rename(new_dir + Subject_Left + file_extension)
                                                logging.info(f'Renamed {path_lv} to {Subject_Left}')
                                                count_vc_lrv = 1 
                                            else:
                                                path_lv.rename(new_dir + Subject_Left +"_{0}".format(count_vc_lrv)+ file_extension)
                                                logging.info(f"Renamed {path_lv} to {Subject_Left}_{count_lrv}")
                                                count_vc_lrv += 1
                                        else:
                                            if count_lv == '':
                                                path_lv.rename(new_dir + Subject_Left + file_extension)
                                                logging.info(f'Renamed {path_lv} to {Subject_Left}')
                                                count_lv = 1 
                                            else:
                                                path_lv.rename(new_dir + Subject_Left +"_{0}".format(count_lv)+ file_extension)
                                                logging.info(f'Renamed {path_lv} to {Subject_Left}_{count_lv}')
                                                count_lv += 1
                                    elif avg_prediction == 3:
                                        path_rv = pathlib.Path(key)
                                        if portal == 'RED BELL' or portal not in naming_data:
                                            path_rv.rename(new_dir + Subject_Right +"_{0}".format(count_lrv)+ file_extension)
                                            logging.info(f"Renamed {path_rv} to {Subject_Right}_{count_lrv}")
                                            count_lrv += 1
                                        elif portal == 'Valuation Connect':
                                            if count_vc_lrv == '':
                                                path_rv.rename(new_dir + Subject_Right + file_extension)
                                                logging.info(f'Renamed {path_rv} to {Subject_Right}')
                                                count_vc_lrv = 1
                                            else:
                                                path_rv.rename(new_dir + Subject_Right +"_{0}".format(count_vc_lrv)+ file_extension)
                                                logging.info(f"Renamed {path_rv} to {Subject_Right}_{count_vc_lrv}")
                                                count_vc_lrv += 1
                                        else:
                                            if count_rv == '':
                                                path_rv.rename(new_dir + Subject_Right + file_extension)
                                                logging.info(f'Renamed {path_rv} to {Subject_Right}')
                                                count_rv = 1
                                            else:
                                                path_rv.rename(new_dir + Subject_Right +"_{0}".format(count_rv)+ file_extension)
                                                logging.info(f'Renamed {path_rv} to {Subject_Right}_{count_rv}')
                                                count_rv += 1
                                    elif avg_prediction == 4:
                                        path_ss = pathlib.Path(key)
                                        if count_ss == '':
                                            path_ss.rename(new_dir + Subject_Street_Sign + file_extension)
                                            logging.info(f'Renamed {path_ss} to {Subject_Street_Sign}')
                                            count_ss = 1
                                        else:
                                            path_ss.rename(new_dir + Subject_Street_Sign +"_{0}".format(count_ss) + file_extension)
                                            logging.info(f"Renamed {path_ss} to {Subject_Street_Sign}_{count_ss}")
                                            count_ss += 1
                                    elif avg_prediction == 5:
                                        path_sv = pathlib.Path(key)
                                        if SView_Type == 'Type-1':
                                            logging.info('Type 1 Street Entered')
                                            path_sv.rename(new_dir + Subject_Street  +"_{0}".format(count_sv)+ file_extension)
                                            logging.info(f"Renamed {path_sv} to {Subject_Street}_{count_sv}")
                                            count_sv  += 1
                                        elif SView_Type == 'Type-2':
                                            logging.info('Type 2 Street Entered')
                                            if count_t2_sv == '':
                                                path_sv.rename(new_dir + Subject_Street + file_extension)
                                                logging.info(f'Renamed {path_sv} to {Subject_Street}') 
                                                count_t2_sv = 1
                                            else:
                                                path_sv.rename(new_dir + Subject_Street +"_{0}".format(count_t2_sv) + file_extension)
                                                logging.info(f"Renamed {path_sv} to {Subject_Street}_{count_t2_sv}")
                                                count_t2_sv += 1
                                        elif SView_Type == 'Type-3':
                                            logging.info('Type 3 Street Entered')
                                            if count_sv % 2 != 0:
                                                if count_rsv == '':
                                                    path_sv.rename(new_dir + Subject_Street + file_extension)
                                                    logging.info(f'Renamed {path_sv} to {Subject_Street}') 
                                                    count_rsv = 1
                                                    count_sv += 1
                                                else:
                                                    path_sv.rename(new_dir + Subject_Street +"_{0}".format(count_rsv) + file_extension)
                                                    logging.info(f"Renamed {path_sv} to {Subject_Street}_{count_rsv}")
                                                    count_rsv += 1
                                                    count_sv += 1
                                            else:
                                                if count_lsv == '':
                                                    path_sv.rename(new_dir + Subject_Street1 + file_extension)
                                                    logging.info(f'Renamed {path_sv} to {Subject_Street1}') 
                                                    count_lsv = 1
                                                    count_sv += 1
                                                else:
                                                    path_sv.rename(new_dir + Subject_Street1 +"_{0}".format(count_lsv) + file_extension)
                                                    logging.info(f"Renamed {path_sv} to {Subject_Street1}_{count_lsv}")
                                                    count_lsv += 1
                                                    count_sv += 1
                                        elif SView_Type == 'Type-4':
                                            logging.info('Type 4 Street Entered')
                                            path_sv.rename(new_dir + Subject_Street  +"{0}".format(count_sv)+ file_extension)
                                            logging.info(f"Renamed {path_sv} to {Subject_Street}_{count_sv}")
                                            count_sv  += 1
                                    else:
                                        print('Not Verified!')
                                        logging.error(f"Not Verified with prediction as {avg_prediction}")
                                except Exception as e:
                                    logging.info(f'Duplicate Error 5 {str(e)}')
                                    continue


                    all_files = os.listdir(new_dir)
                    matched_files = [file for file in all_files if file.endswith(suffix)]
                    new_count = 0
                    for file_name in matched_files:
                        new_renamed_paths[new_count] = os.path.join(new_dir, file_name)
                        new_count += 1

                    for i in range (0, len(new_renamed_paths)):
                        img_path = new_renamed_paths[i]
                        new_path = os.path.splitext(img_path)
                        path = new_path[0]
                        extension = new_path[1]
                        if portal == 'Single Source' or portal == 'RRReview':
                            if extension == '.jpg' or extension == '.JPG':
                                continue
                            else:
                                try:
                                    base_file_path = path + extension
                                    base_jpg_file_path = path + ".jpg"
                                    convert_to_jpg(base_file_path, base_jpg_file_path)
                                    logging.info(f'Converted {img_path} to jpg')
                                    os.remove(base_file_path) 
                                except Exception as e:
                                    logging.error(f'JPG Conversion Error occurred: {str(e)}')
                                    continue
                        else:
                            if extension == '.jpeg':
                                continue
                            elif(extension == '.png'):
                                try:
                                    png_file_path = path + extension
                                    jpeg_file_path = path + ".jpeg"
                                    convert_png_to_jpeg(png_file_path, jpeg_file_path)
                                    logging.info(f'Converted {img_path} to jpeg')
                                    os.remove(png_file_path)
                                except Exception as e:
                                    logging.error(f'Conversion Error occurred: {str(e)}')
                                    continue
                            else:
                                try:
                                    base_file_path = path + extension
                                    base_jpeg_file_path = path + ".jpeg"
                                    convert_to_jpeg(base_file_path, base_jpeg_file_path)
                                    logging.info(f'Converted {img_path} to jpeg')
                                    os.remove(base_file_path) 
                                except Exception as e:
                                    logging.error(f'Conversion Error occurred: {str(e)}')
                                    continue

                    all_files = os.listdir(new_dir)
                    matched_files = [file for file in all_files if file.endswith(suffix)]
                    new_count = 0

                    for file_name in matched_files:
                        new_renamed_paths1[new_count] = os.path.join(new_dir, file_name)
                        new_count += 1

                    for i in range(0, len(new_renamed_paths1)):
                        img_path = new_renamed_paths1[i]
                        if portal == "Class Valuation":
                            max_size = 2 * 1024 * 1024  
                            output_file_path = img_path
                            compress_image(img_path, output_file_path, max_size)
                            
                        elif portal == "RRReview":
                            max_size = 25 * 1024 * 1024
                            output_file_path = img_path 
                            compress_image(img_path, output_file_path, max_size)

                        base_path = os.path.dirname(os.path.dirname(img_path))
                        new_json_path = base_path + '\client_detail.json'

                        if img_test == '':
                            img_test = base_path
                        elif img_test != base_path:
                            img_test = base_path
                            test_flag = False                        

                        with open(new_json_path, 'r') as file:
                            data = json.loads(file.read())
                        input_datetime_str = data['order_date']
                        test_due_date = data['due_date']
                        portal = data['portal_name']

                        if portal == 'RED BELL':
                            test_due_date1 = datetime.strptime(test_due_date, '%Y-%m-%dT%H:%M:%S.%f')
                            due_date = test_due_date1.strftime('%m/%d/%y %I:%M:%S %p')
                            due_date = due_date.split(' ')
                            due_date_str = due_date[0]
                            due_time_str = ' '.join(due_date[1:])


                            due_date = datetime.strptime(due_date_str, '%m/%d/%y').date()
                            due_time = datetime.strptime(due_time_str, '%I:%M:%S %p').time()
                            due_time24 = datetime.strptime(due_time_str, '%I:%M:%S %p')

                            from_tz = pytz.timezone('Asia/Kolkata')
                            to_tz = pytz.timezone('America/Denver')


                            input_datetime_str = input_datetime_str.split('.')[0]
                            input_datetime = datetime.strptime(input_datetime_str, '%Y-%m-%dT%H:%M:%S')
                            input_datetime = from_tz.localize(input_datetime)
                            output_datetime = input_datetime.astimezone(to_tz)
                            output_datetime_str = output_datetime.strftime('%m-%d-%Y %H:%M:%S')

                            test = output_datetime_str.split(' ')
                            date = test[0]
                            order_time_split = test[1]
                            split = order_time_split.split(':')
                            hour = int(split[0])


                            date_strip = datetime.strptime(date, '%m-%d-%Y')
                            next_day_temp = date_strip + timedelta(days=1)
                            next_day = next_day_temp.strftime('%m-%d-%Y')
                            same_day = date_strip.strftime('%m-%d-%Y')


                            if test_flag == False:
                                if hour < 12:
                                    new_hour = str(random.randint(8, 11)).zfill(2)
                                    new_minutes = str(random.randint(0, 59)).zfill(2)
                                    new_seconds = str(random.randint(0, 59)).zfill(2)
                                    order_time = new_hour + ':' + new_minutes + ':' + new_seconds
                                    meta_time_str = order_time
                                    meta_time = datetime.strptime(meta_time_str, "%H:%M:%S")
                                    test_flag = True
                                else:
                                    new_hour = str(random.randint(14,16)).zfill(2)
                                    new_minutes = str(random.randint(0,59)).zfill(2)
                                    new_seconds = str(random.randint(0,59)).zfill(2)
                                    order_time = new_hour + ':'+new_minutes+':'+new_seconds
                                    meta_time_str = order_time
                                    meta_time = datetime.strptime(meta_time_str, "%H:%M:%S")
                                    test_flag = True


                            if due_date < next_day_temp.date() or (due_date == next_day_temp.date() and due_time < datetime.strptime('11:00:00 AM', '%I:%M:%S %p').time()):
                                meta_date= same_day
                            else:
                                meta_date= next_day


                            next_day = datetime.strptime(next_day, '%m-%d-%Y')
                            due_date = datetime.strptime(due_date_str, '%m/%d/%y')

                            while (next_day - due_date).days > 0:
                                next_day = next_day - timedelta(days=1)
                                next_day_str = next_day.strftime('%m-%d-%Y')
                                meta_date = next_day_str

                            random_variable = random.randint(0,1)

                            if random_variable == 1:
                                meta_time = meta_time + timedelta(minutes=1)

                            final_meta_time = meta_time.strftime("%H:%M:%S")

                            if next_day == due_date:
                                time_difference = due_time24 - meta_time
                                if abs(time_difference.total_seconds()) < 3 * 3600: 
                                    new_meta_time = meta_time - timedelta(hours=3)
                                    final_meta_time = new_meta_time.strftime('%H:%M:%S')
                            change_date_taken(img_path, meta_date, final_meta_time)
                            logging.info(f'Added metadata  to {img_path}: {meta_date} {final_meta_time}')

                            exif_data = get_exif_data(img_path)

                            exif_json_data = {img_path: exif_data}
                            exif_json_path = os.path.join(base_path, "exif_data.json")

                            if os.path.exists(exif_json_path):
                                with open(exif_json_path, "r") as json_file:
                                    existing_data = json.load(json_file)
                            else:
                                existing_data = {}

                            existing_data.update(exif_json_data)

                            with open(exif_json_path, "w") as json_file:
                                json.dump(existing_data, json_file, indent=4)
                        else:
                            logging.info(f'Metadata not added for {base_path} since portal is {portal}')                   

                    preds_1 = {}
                    preds_2 = {}
                    preds_3 = {}

                    folder_count+=1 
                    print('Folder Count :',folder_count,'/',base_folder_length)
                    print("Next Folder")
        else:
            time.sleep(30)
except Exception as e:
    error_value = f'{e}'
    if error_value == "APIError: [500]: Internal error encountered." or error_value == "APIError: [503]: The service is currently unavailable." or error_value == "('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))" or error_value == "Expecting value: line 1 column 1 (char 0)":
        logging.error(f'Restartable ERROR Occured : {error_value}. Restarting the program now!!')
        logging.error(traceback.print_exc())
        restart_program()
    else:
        logging.error(f'Main PropVision Error occurred: {str(e)}')
        print(error_value)
        logging.error(traceback.print_exc())
        send_error_email(error_value)
            

# In[ ]:




