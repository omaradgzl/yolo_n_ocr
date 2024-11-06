DB_USERNAME = "***"
DB_PASSWORD = "***"
DB_HOST = "***"
DB_PORT = "***"
DB_NAME = "***"
ENDPOINT = '***'

from ultralytics import YOLO 
import requests
import numpy as np
import cv2
import pandas as pd
from difflib import SequenceMatcher
from PIL import Image
from io import BytesIO
from dbConn import DbConnection
from mmocr.apis import MMOCRInferencer
import hashlib
import warnings
warnings.filterwarnings("ignore")

def main(last_month):

    query = """ **** """.format(last_month)

    model_serial = YOLO('bestLarge.pt')
    model_kafk = YOLO('yoloV8.pt')

    inferencer = MMOCRInferencer(det = 'dbnetpp', rec = 'abinet', kie = 'SDMGR')

    conn = DbConnection(username= DB_USERNAME, password= DB_PASSWORD, host = DB_HOST, port= DB_PORT, name = DB_NAME)

    df = conn.getDataFrame(query=query)

    df_list = df.values.tolist()

    df_final_serial = pd.DataFrame(columns = ['***'])
    
    df_final_kafk = pd.DataFrame(columns = ['***'])
    
    df_final_dup = pd.DataFrame(columns = ['***'])
    


    def similarity(a,b):
        seq = SequenceMatcher(a=a, b=b)
        return seq.ratio()    

    for index,info in enumerate(df_list):
        try:
            response = requests.get(ENDPOINT + str(info[0]))
            data = response.json()

            if len(data['orders']) > 0 :
                
                for worder in data['orders']:
                    worder_no = worder.get('order_no')

                    if str(worder_no) == str(info[4]):

                        isUnoDetected = False
                        isDuoDetected = False
                        resim_list = worder.get('resimler')
                        tarih = worder.get('date')[:8]


                        if len(resim_list) > 0:
                            for resim in resim_list:

                                response = requests.get(resim)
                                image = Image.open(BytesIO(response.content))
                                image_asarray = np.asarray(image)
                                results_serial = model_serial(image_asarray)
                                results_kafk = model_kafk(image_asarray)
                                
                                image_hash = hashlib.md5(response.content).hexdigest()
                                df_final_dup.loc[len(df_final_dup)] = [info[0], worder_no, tarih, info[7], resim, image_hash]
                                
                                if len(results_serial[0].boxes.cls) > 0 :
                                    list_top = results_serial[0].boxes.xyxy[0].tolist() #top left bottom right
                                    points = [[list_top[0], list_top[1]],[list_top[2], list_top[1]],[list_top[2], list_top[3]],[list_top[0], list_top[3]]]
                                    contours = np.array(points)
                                    mask = np.zeros(image_asarray.shape, dtype = np.uint8)
                                    cv2.fillPoly(mask, pts=np.int32([contours]), color = (255,255,255))
                                    masked_image = cv2.bitwise_and(image_asarray, mask)
                                    result_text = inferencer(masked_image, save_vis = False)
                                    text = result_text['predictions'][0]['rec_texts']
                                    new_text = ''.join(text)
                                    the_text = ''

                                    for idx,char in enumerate(new_text):
                                        if char.isdigit():
                                            the_text+= char
                                    pred = the_text

                                    str_uno = str(info[9])
                                    match = SequenceMatcher(None, str_uno, pred).find_longest_match(0, len(str_uno), 0, len(pred))
                                    matched_sub = pred[match.b:match.b + match.size]

                                    ratio = (len(matched_sub)/len(str_uno))
                                    
                                    df_final_serial.loc[len(df_final_serial)] = [info[0], info[1], info[2], info[3], info[4], info[5], info[6], info[7], info[8], info[9], pred, ratio, resim]
                                    print('For {}_{} serial photo processed.'.format(info[0], worder_no))
                                if len(results_kafk[0].boxes.cls) >= 2 :
                                        isUnoDetected = True
                                        isDuoDetected = True
                                        print("Subscriber : {} , worder : {}, both found exiting work order.".format(info[0],worder_no))
                                        df_final_kafk.loc[len(df_final_kafk)] = [info[0],worder_no,info[6],tarih,isUnoDetected,isDuoDetected,resim]
                                        break
                                    
                                elif len(results_kafk[0].boxes.cls) == 1 :
                                
                                    if isUnoDetected == False :
                                        if 0 in results_kafk[0].boxes.cls :
                                            isUnoDetected = True
                                            # isDuoDetected = False
                                            df_final_kafk.loc[len(df_final_kafk)] = [info[0],worder_no,info[6],tarih,isUnoDetected,isDuoDetected,resim]
                                            print("Subscriber : {} , worder : {}, Uno found, no longer checking Unos.".format(info[0],worder_no))     
                                            continue

                                    if isDuoDetected == False:
                                        if 1 in results_kafk[0].boxes.cls :
                                            # isUnoDetected = False
                                            isDuoDetected = True
                                            print("Subscriber : {} , worder : {}, Duo found, no longer checking Duos.".format(info[0],worder_no))
                                            df_final_kafk.loc[len(df_final_kafk)] = [info[0],worder_no,info[6],tarih,isUnoDetected,isDuoDetected,resim]
                                            continue

                                # if isUnoDetected and isDuoDetected:
                                #     break
                                            
                                if isUnoDetected == False and isDuoDetected == False:
                                    df_final_kafk.loc[len(df_final_kafk)] = [info[0],worder_no,info[6],tarih,isUnoDetected,isDuoDetected,resim]
                                    print("Subscriber : {} , worder : {}, nothing found, adding last image of worder.".format(info[0],worder_no))
        except Exception as e : 
            print(e)


    df_old = conn.getDataFrame('SELECT * FROM KAFK_DUP')
    df_final_dup = pd.concat([df_old, df_final_dup], axis = 0) 
    list_hash = df_final_dup['photohash'].values.tolist() 
    dup_index = []
    dup_same_tesisat_index = []

    for hash in list_hash:
        df_sub = df_final_dup[df_final_dup['photohash'] == hash]  
        if len(df_sub['subs'].unique()) > 1 :
            dup_index += df_sub.index.values.tolist()     
        if len(df_sub['subs'].unique()) == 1 :
            if(len(df_sub['worder_no'].unique())) > 1: 
                dup_same_tesisat_index += df_sub.index.values.tolist()

    dup_index = list(set(dup_index))  
    dup_same_tesisat_index = list(set(dup_same_tesisat_index))  



    df_final_dup['diff_1'] = 0
    df_final_dup['diff_2'] = 0

    df_final_dup.loc[dup_index, 'diff_1'] = 1
    df_final_dup.loc[dup_same_tesisat_index, 'diff_2'] = 1

    df_final_serial['ratio'] = df_final_serial['ratio'] * 100
    filename_serial = 'kafk-serial_'+last_month+'.xlsx'
    filename_kafk = 'kafk_'+last_month+'.xlsx'
    filename_dup = 'kafk-dup_' + last_month + '.xlsx'

    df_final_serial.to_excel(filename_serial, index = False)
    df_final_kafk.to_excel(filename_kafk, index = False)
    df_final_dup.to_excel(filename_dup, index = False)

    return filename_kafk,filename_serial, filename_dup

# Think I didn't take risks to get to this 'sitch?
# I've been scummy, I admit
# I did what I did, but I deserve this
