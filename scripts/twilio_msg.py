import os
from twilio.rest import Client 



class TwilioMessageError:
    def __init__(self, account_id, auth_token):
        self.account_id = account_id
        self.auth_token = auth_token


    def send_message_exception(self, message):
        client = Client(self.account_id, self.auth_token, region='us1') 
        text = client.messages.create( 
                    from_='whatsapp:+14155238886',
                    body= f'{message}',
                    to='whatsapp:+5511976860267'
                    ) 
 
        print(text.sid)

app = TwilioMessageError(os.environ.get('SID_TWILIO'), os.environ.get('TOKEN_TWILIO'))
app.send_message_exception('Teste de envio de mensagem de exceções do sistema.')