import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import emailParams


def sendEmail(schema,html,subject):
    msg=MIMEMultipart()
    msg['From']=emailParams.SEND_FROM
    server=smtplib.SMTP_SSL('smtp.gmail.com',465)
    title = f'{schema}_' + subject
    msg['SUBJECT'] = title
    if type(emailParams.SEND_TO)==list:
        msg['To']=','.join(emailParams.SEND_TO)
    else:
        msg['To']=emailParams.SEND_TO
    #print(subject)
    
    try:
        
        #server.ehlo()
        #server.starttls()
        print(emailParams.HOST)
        print(emailParams.PORT)
        
        server.login(msg['From'], emailParams.PASSWORD)
    except Exception as err:
        print('There is an error ',err)
    else:
        '''for i,html in enumerate(html_list):
            if i==0:s
                htm=html+'<br>'
            else:
                htm+=html'''
        attachment=MIMEText(html,'html')
        
        msg.attach(attachment)
        server.sendmail(msg['From'],msg['To'],msg.as_string())
        server.quit()