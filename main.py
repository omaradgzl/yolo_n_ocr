USERNAME = "***"
PASSWORD = "***"
SERVER = "***"
PORT = "***"


from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import smtplib
import detectorV8
import ultimate
import excelToDb
import combined
import datetime



def getDate():
    today = datetime.date.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    return last_month.strftime("%Y%m")


if __name__ == '__main__':

    date_to_get = getDate()


    file_kafk,file_serial, file_dup = combined.main(last_month = date_to_get)
    

    dict_month = {'01': 'Ocak',
                '02': 'Şubat',
                '03': 'Mart',
                '04': 'Nisan',
                '05': 'Mayıs',
                '06': 'Haziran',
                '07': 'Temmuz',
                '08': 'Ağustos',
                '09': 'Eylül',
                '10': 'Ekim',
                '11': 'Kasım',
                '12': 'Aralık'}


    msg = MIMEMultipart()
    msg['From'] = USERNAME
    msg['To'] = '***'
    cc_recipients=['***']
    msg['Cc'] = ', '.join(cc_recipients)
    # msg['Bcc'] = '***'
    msg['Subject'] = 'KAFK Hk.'
    message = """Merhabalar,
Raporlar {} ayı için ektedir.

Teşekkürler,
İyi Çalışmalar.

    """.format(dict_month[date_to_get[4:]])
    msg.attach(MIMEText(message, 'plain'))

    # E-postaya belgeyi ekleyin
    with open(file_kafk, 'rb') as file:
        attachment = MIMEApplication(file.read(), Name=file_kafk)
        attachment['Content-Disposition'] = 'attachment; filename="%s"' %file_kafk
        msg.attach(attachment)

    with open(file_serial, 'rb') as file:
        attachment = MIMEApplication(file.read(), Name=file_serial)
        attachment['Content-Disposition'] = 'attachment; filename="%s"' %file_serial
        msg.attach(attachment)

    with open(file_dup, 'rb') as file:
        attachment = MIMEApplication(file.read(), Name=file_dup)
        attachment['Content-Disposition'] = 'attachment; filename="%s"' %file_dup
        msg.attach(attachment)

    try:
        server = smtplib.SMTP(SERVER, PORT)
        server.starttls()
        server.login(USERNAME, PASSWORD)
        server.send_message(msg)
        server.quit()
        print("E-posta gönderildi ✉️")
    except Exception as e:
        print("E-posta gönderirken bir hata oluştu:", str(e))
        

    reply = excelToDb.main(file_kafk,file_serial, file_dup)

    msg = MIMEMultipart()
    msg['From'] = USERNAME
    msg['To'] = '***'
    msg['Subject'] = 'Kesme Fotoğraf Kontrolü Rapor Hk.'

    # E-posta metnini ekleyin
    message = reply
    msg.attach(MIMEText(message, 'plain'))
    try:
        server = smtplib.SMTP(SERVER, PORT)
        server.starttls()
        server.login(USERNAME, PASSWORD)
        server.send_message(msg)
        server.quit()
        print("Sahip E-postası gönderildi ✉️")
    except Exception as e:
        print("Sahip E-postası gönderilirken bir hata oluştu:", str(e))


